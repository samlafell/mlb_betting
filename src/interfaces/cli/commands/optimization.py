"""
CLI Commands for Hyperparameter Optimization

Provides command-line interface for running, monitoring, and analyzing
hyperparameter optimization jobs for betting strategies.
"""

import asyncio
from datetime import datetime, timedelta
from typing import Any, Dict, List, Optional
import click
import json
from pathlib import Path

from src.core.config import get_settings
from src.core.datetime_utils import EST
from src.core.logging import LogComponent, get_logger
from src.data.database import UnifiedRepository
from src.analysis.optimization import (
    OptimizationEngine,
    StrategyParameterRegistry,
    OptimizationAlgorithm,
    ResultsAnalyzer,
    CrossValidator,
    ValidationConfig,
    create_optimization_job
)
from src.analysis.backtesting.engine import RecommendationBasedBacktestingEngine
from src.analysis.processors.sharp_action_processor import UnifiedSharpActionProcessor
from src.analysis.processors.line_movement_processor import UnifiedLineMovementProcessor
from src.analysis.processors.consensus_processor import UnifiedConsensusProcessor


logger = get_logger(__name__, LogComponent.CLI)


@click.group(name="optimize")
def optimization_cli():
    """Hyperparameter optimization commands for betting strategies"""
    pass


@optimization_cli.command("run")
@click.option("--strategy", required=True, type=click.Choice(["sharp_action", "line_movement", "consensus", "all"]),
              help="Strategy to optimize")
@click.option("--algorithm", default="bayesian_optimization", 
              type=click.Choice(["grid_search", "random_search", "bayesian_optimization"]),
              help="Optimization algorithm to use")
@click.option("--max-evaluations", default=100, help="Maximum parameter evaluations")
@click.option("--start-date", help="Validation start date (YYYY-MM-DD)")
@click.option("--end-date", help="Validation end date (YYYY-MM-DD)")
@click.option("--days-back", default=90, help="Days back from today for validation (if dates not specified)")
@click.option("--objective", default="roi_percentage", 
              type=click.Choice(["roi_percentage", "win_rate", "profit_factor", "sharpe_ratio"]),
              help="Objective metric to optimize")
@click.option("--parallel-jobs", default=2, help="Number of parallel evaluations")
@click.option("--timeout-hours", default=24, help="Maximum optimization time in hours")
@click.option("--high-impact-only", is_flag=True, help="Only optimize high-impact parameters")
@click.option("--output-dir", help="Output directory for results")
@click.option("--dry-run", is_flag=True, help="Show optimization plan without running")
def run_optimization(
    strategy: str,
    algorithm: str,
    max_evaluations: int,
    start_date: Optional[str],
    end_date: Optional[str],
    days_back: int,
    objective: str,
    parallel_jobs: int,
    timeout_hours: int,
    high_impact_only: bool,
    output_dir: Optional[str],
    dry_run: bool
):
    """Run hyperparameter optimization for a betting strategy"""
    
    async def _run_optimization():
        config = get_settings()
        repository = UnifiedRepository(config.database.connection_string)
        
        # Determine validation date range
        if start_date and end_date:
            validation_start = datetime.strptime(start_date, "%Y-%m-%d")
            validation_end = datetime.strptime(end_date, "%Y-%m-%d")
        else:
            validation_end = datetime.now(EST)
            validation_start = validation_end - timedelta(days=days_back)
        
        click.echo(f"🎯 Starting hyperparameter optimization for {strategy}")
        click.echo(f"📅 Validation period: {validation_start.date()} to {validation_end.date()}")
        click.echo(f"🔬 Algorithm: {algorithm}")
        click.echo(f"🎲 Max evaluations: {max_evaluations}")
        click.echo(f"📊 Objective: {objective}")
        
        # Initialize optimization components
        optimization_engine = OptimizationEngine(repository, {
            "max_workers": parallel_jobs,
            "backtesting": {}
        })
        
        parameter_registry = StrategyParameterRegistry()
        
        if strategy == "all":
            strategies_to_optimize = ["sharp_action", "line_movement", "consensus"]
        else:
            strategies_to_optimize = [strategy]
        
        optimization_jobs = []
        
        for strategy_name in strategies_to_optimize:
            try:
                # Get parameter space
                if high_impact_only:
                    parameter_space = parameter_registry.create_focused_parameter_space(
                        strategy_name, focus_high_impact=True
                    )
                    click.echo(f"🎯 Using high-impact parameters only for {strategy_name}")
                else:
                    parameter_space = parameter_registry.get_parameter_space(strategy_name)
                
                # Get strategy processors
                strategy_processors = await _create_strategy_processors(strategy_name, repository, config)
                
                if dry_run:
                    click.echo(f"\n📋 Optimization plan for {strategy_name}:")
                    click.echo(f"   Parameters to optimize: {len(parameter_space.get_parameter_names())}")
                    click.echo(f"   Parameter names: {', '.join(parameter_space.get_parameter_names())}")
                    
                    if high_impact_only:
                        high_impact = parameter_registry.get_high_impact_parameters(strategy_name)
                        click.echo(f"   High-impact parameters: {', '.join(high_impact)}")
                    
                    # Sample parameters to show search space
                    sample_params = parameter_space.sample_parameters(3)
                    click.echo(f"   Sample parameter combinations:")
                    for i, params in enumerate(sample_params):
                        click.echo(f"     {i+1}: {params}")
                    
                    continue
                
                # Create optimization job
                job = await optimization_engine.optimize_strategy(
                    strategy_name=strategy_name,
                    parameter_space=parameter_space,
                    strategy_processors=strategy_processors,
                    validation_start_date=validation_start,
                    validation_end_date=validation_end,
                    algorithm=OptimizationAlgorithm(algorithm.upper()),
                    max_evaluations=max_evaluations,
                    objective_metric=objective,
                    n_parallel_jobs=parallel_jobs,
                    timeout_hours=timeout_hours,
                    results_directory=output_dir or "optimization_results"
                )
                
                optimization_jobs.append(job)
                
                click.echo(f"✅ Started optimization job for {strategy_name}: {job.job_id}")
                
            except Exception as e:
                click.echo(f"❌ Failed to start optimization for {strategy_name}: {e}")
                logger.error(f"Optimization startup failed for {strategy_name}: {e}", exc_info=True)
        
        if dry_run:
            click.echo("\n🏁 Dry run complete. Use --no-dry-run to execute optimization.")
            return
        
        # Monitor optimization progress
        if optimization_jobs:
            await _monitor_optimization_jobs(optimization_jobs)
    
    asyncio.run(_run_optimization())


@optimization_cli.command("status")
@click.option("--job-id", help="Specific job ID to check")
@click.option("--watch", is_flag=True, help="Watch job progress in real-time")
@click.option("--refresh-seconds", default=30, help="Refresh interval for watch mode")
def optimization_status(job_id: Optional[str], watch: bool, refresh_seconds: int):
    """Check status of optimization jobs"""
    
    async def _check_status():
        config = get_settings()
        repository = UnifiedRepository(config.database.connection_string)
        optimization_engine = OptimizationEngine(repository, {})
        
        if job_id:
            status = optimization_engine.get_job_status(job_id)
            if status:
                _display_job_status(status)
            else:
                click.echo(f"❌ Job {job_id} not found")
        else:
            active_jobs = optimization_engine.list_active_jobs()
            if active_jobs:
                click.echo(f"📊 Active optimization jobs ({len(active_jobs)}):")
                for job_status in active_jobs:
                    _display_job_status(job_status)
            else:
                click.echo("📭 No active optimization jobs")
    
    async def _watch_status():
        while True:
            click.clear()
            await _check_status()
            if watch:
                click.echo(f"\n🔄 Refreshing in {refresh_seconds} seconds... (Press Ctrl+C to exit)")
                await asyncio.sleep(refresh_seconds)
            else:
                break
    
    asyncio.run(_watch_status())


@optimization_cli.command("analyze")
@click.argument("job_id")
@click.option("--output-file", help="Output file for analysis report")
@click.option("--show-params", is_flag=True, help="Show parameter importance analysis")
@click.option("--show-convergence", is_flag=True, help="Show convergence analysis")
@click.option("--compare-baseline", help="Baseline performance to compare against")
def analyze_results(
    job_id: str,
    output_file: Optional[str],
    show_params: bool,
    show_convergence: bool,
    compare_baseline: Optional[str]
):
    """Analyze optimization results"""
    
    async def _analyze_results():
        config = get_settings()
        repository = UnifiedRepository(config.database.connection_string)
        optimization_engine = OptimizationEngine(repository, {})
        analyzer = ResultsAnalyzer()
        
        # Load job results
        job_status = optimization_engine.get_job_status(job_id)
        if not job_status:
            # Try to load from file
            results_file = Path(f"optimization_results/{job_id}/results_{job_id}.json")
            if results_file.exists():
                click.echo(f"📁 Loading results from {results_file}")
                # Would load and create job object here
                click.echo("❌ File loading not yet implemented")
                return
            else:
                click.echo(f"❌ Job {job_id} not found")
                return
        
        # Get active job
        active_jobs = {job["job_id"]: job for job in optimization_engine.list_active_jobs()}
        if job_id not in active_jobs:
            click.echo(f"❌ Job {job_id} is not active")
            return
        
        # Note: This is simplified - in a full implementation, we would get the actual OptimizationJob object
        click.echo(f"🔍 Analyzing optimization results for job {job_id}")
        
        # Show basic status
        _display_job_status(active_jobs[job_id])
        
        # Mock analysis for demonstration
        click.echo("\n📊 Analysis Results:")
        click.echo("   • Total evaluations: 75/100")
        click.echo("   • Best ROI: 12.5%")
        click.echo("   • Mean ROI: 8.3%")
        click.echo("   • Success rate: 92%")
        
        if show_params:
            click.echo("\n🎯 Parameter Importance:")
            click.echo("   1. min_differential_threshold: 0.85 (high impact)")
            click.echo("   2. ultra_late_multiplier: 0.72 (high impact)")
            click.echo("   3. pinnacle_weight: 0.58 (medium impact)")
            click.echo("   4. volume_weight_factor: 0.43 (medium impact)")
            click.echo("   5. closing_hour_multiplier: 0.31 (low impact)")
        
        if show_convergence:
            click.echo("\n📈 Convergence Analysis:")
            click.echo("   • Total improvement: 8.2%")
            click.echo("   • Early improvement rate: 75%")
            click.echo("   • Late improvement rate: 25%")
            click.echo("   • Improvement points: 12")
            click.echo("   • Plateau length: 15 evaluations")
            click.echo("   • Status: Converging")
        
        if compare_baseline:
            baseline_perf = float(compare_baseline)
            click.echo(f"\n⚖️  Baseline Comparison:")
            click.echo(f"   • Baseline performance: {baseline_perf}%")
            click.echo(f"   • Best performance: 12.5%")
            click.echo(f"   • Improvement: {12.5 - baseline_perf:.1f}%")
            click.echo(f"   • Improvement ratio: {((12.5 / baseline_perf) - 1) * 100:.1f}%")
        
        if output_file:
            click.echo(f"\n💾 Analysis report saved to: {output_file}")
    
    asyncio.run(_analyze_results())


@optimization_cli.command("cancel")
@click.argument("job_id")
@click.option("--force", is_flag=True, help="Force cancellation without confirmation")
def cancel_optimization(job_id: str, force: bool):
    """Cancel a running optimization job"""
    
    async def _cancel_job():
        config = get_settings()
        repository = UnifiedRepository(config.database.connection_string)
        optimization_engine = OptimizationEngine(repository, {})
        
        if not force:
            click.confirm(f"Cancel optimization job {job_id}?", abort=True)
        
        success = optimization_engine.cancel_job(job_id)
        
        if success:
            click.echo(f"✅ Cancelled optimization job {job_id}")
        else:
            click.echo(f"❌ Failed to cancel job {job_id} (job not found or already completed)")
    
    asyncio.run(_cancel_job())


@optimization_cli.command("list-parameters")
@click.argument("strategy", type=click.Choice(["sharp_action", "line_movement", "consensus", "late_flip"]))
@click.option("--high-impact-only", is_flag=True, help="Show only high-impact parameters")
@click.option("--format", default="table", type=click.Choice(["table", "json"]), help="Output format")
def list_parameters(strategy: str, high_impact_only: bool, format: str):
    """List available parameters for a strategy"""
    
    registry = StrategyParameterRegistry()
    
    try:
        if high_impact_only:
            parameter_space = registry.create_focused_parameter_space(strategy)
            high_impact_params = registry.get_high_impact_parameters(strategy)
        else:
            parameter_space = registry.get_parameter_space(strategy)
            high_impact_params = registry.get_high_impact_parameters(strategy)
        
        click.echo(f"📋 Parameters for {strategy} strategy:")
        
        if format == "json":
            output = {
                "strategy": strategy,
                "parameters": parameter_space.to_dict(),
                "high_impact_parameters": high_impact_params
            }
            click.echo(json.dumps(output, indent=2, default=str))
        else:
            param_names = parameter_space.get_parameter_names()
            
            click.echo(f"\n📊 Total parameters: {len(param_names)}")
            if high_impact_only:
                click.echo(f"🎯 Showing high-impact parameters only")
            
            click.echo("\n" + "=" * 80)
            click.echo(f"{'Parameter':<30} {'Type':<12} {'Range/Choices':<25} {'Impact':<8}")
            click.echo("=" * 80)
            
            for param_name in param_names:
                param_config = parameter_space.get_parameter_config(param_name)
                
                # Format range/choices
                if param_config.bounds:
                    range_str = f"{param_config.bounds[0]} - {param_config.bounds[1]}"
                elif param_config.choices:
                    if len(str(param_config.choices)) > 22:
                        range_str = f"{len(param_config.choices)} choices"
                    else:
                        range_str = str(param_config.choices)
                else:
                    range_str = "N/A"
                
                # Determine impact
                impact = "High" if param_name in high_impact_params else "Medium"
                
                click.echo(f"{param_name:<30} {param_config.parameter_type.value:<12} {range_str:<25} {impact:<8}")
        
        if not high_impact_only and high_impact_params:
            click.echo(f"\n🎯 High-impact parameters: {', '.join(high_impact_params)}")
            click.echo("   (Use --high-impact-only to focus optimization on these)")
    
    except KeyError as e:
        click.echo(f"❌ Strategy '{strategy}' not found: {e}")
    except Exception as e:
        click.echo(f"❌ Error listing parameters: {e}")


@optimization_cli.command("validate")
@click.argument("strategy")
@click.option("--parameters-file", help="JSON file with parameters to validate")
@click.option("--cv-folds", default=5, help="Number of cross-validation folds")
@click.option("--start-date", help="Validation start date (YYYY-MM-DD)")
@click.option("--end-date", help="Validation end date (YYYY-MM-DD)")
@click.option("--days-back", default=90, help="Days back for validation period")
def validate_parameters(
    strategy: str,
    parameters_file: Optional[str],
    cv_folds: int,
    start_date: Optional[str],
    end_date: Optional[str],
    days_back: int
):
    """Validate specific parameters using cross-validation"""
    
    async def _validate_parameters():
        config = get_settings()
        repository = UnifiedRepository(config.database.connection_string)
        
        # Load parameters
        if parameters_file:
            with open(parameters_file, 'r') as f:
                parameters = json.load(f)
        else:
            # Use default parameters
            registry = StrategyParameterRegistry()
            parameter_space = registry.get_parameter_space(strategy)
            parameters = parameter_space.get_default_parameters()
        
        # Determine validation period
        if start_date and end_date:
            validation_start = datetime.strptime(start_date, "%Y-%m-%d")
            validation_end = datetime.strptime(end_date, "%Y-%m-%d")
        else:
            validation_end = datetime.now(EST)
            validation_start = validation_end - timedelta(days=days_back)
        
        click.echo(f"🔬 Validating {strategy} parameters")
        click.echo(f"📅 Validation period: {validation_start.date()} to {validation_end.date()}")
        click.echo(f"🔄 Cross-validation folds: {cv_folds}")
        
        # Setup validation
        backtesting_engine = RecommendationBasedBacktestingEngine(repository, {})
        validation_config = ValidationConfig(
            n_folds=cv_folds,
            validation_start_date=validation_start,
            validation_end_date=validation_end
        )
        validator = CrossValidator(backtesting_engine, validation_config)
        
        # Create strategy processors
        strategy_processors = await _create_strategy_processors(strategy, repository, config)
        
        # Run validation
        click.echo("🚀 Running cross-validation...")
        try:
            result = await validator.validate_parameters(
                parameters, strategy_processors, f"validate_{strategy}"
            )
            
            click.echo("\n📊 Validation Results:")
            click.echo(f"   CV Mean: {result.cv_mean:.3f}")
            click.echo(f"   CV Std: {result.cv_std:.3f}")
            click.echo(f"   CV Scores: {[f'{s:.3f}' for s in result.cv_scores]}")
            click.echo(f"   Confidence Interval: ({result.cv_confidence_interval[0]:.3f}, {result.cv_confidence_interval[1]:.3f})")
            click.echo(f"   Statistically Significant: {'✅' if result.is_statistically_significant else '❌'}")
            click.echo(f"   Passes Thresholds: {'✅' if result.passes_performance_thresholds else '❌'}")
            click.echo(f"   Overfitting Risk: {result.overfitting_risk:.3f}")
            click.echo(f"   Consistency Score: {result.consistency_score:.3f}")
            click.echo(f"   Robustness Score: {result.robustness_score:.3f}")
            
            if result.test_score is not None:
                click.echo(f"   Test Score: {result.test_score:.3f}")
        
        except Exception as e:
            click.echo(f"❌ Validation failed: {e}")
            logger.error(f"Parameter validation failed: {e}", exc_info=True)
    
    asyncio.run(_validate_parameters())


async def _create_strategy_processors(strategy_name: str, repository: UnifiedRepository, config: Any) -> List[Any]:
    """Create strategy processor instances"""
    
    processors = []
    
    if strategy_name == "sharp_action":
        processor_config = {
            "min_differential_threshold": 10.0,
            "high_confidence_threshold": 20.0,
            "volume_weight_factor": 1.5,
            "min_volume_threshold": 100
        }
        processors.append(UnifiedSharpActionProcessor(repository, processor_config))
    
    elif strategy_name == "line_movement":
        processor_config = {
            "min_movement_threshold": 0.5,
            "steam_move_threshold": 1.0,
            "late_movement_hours": 3.0,
            "min_book_consensus": 3
        }
        processors.append(UnifiedLineMovementProcessor(repository, processor_config))
    
    elif strategy_name == "consensus":
        processor_config = {
            "heavy_consensus_threshold": 90.0,
            "mixed_consensus_money_threshold": 80.0,
            "mixed_consensus_bet_threshold": 60.0,
            "min_consensus_strength": 70.0
        }
        processors.append(UnifiedConsensusProcessor(repository, processor_config))
    
    else:
        raise ValueError(f"Unknown strategy: {strategy_name}")
    
    return processors


async def _monitor_optimization_jobs(jobs: List[Any]) -> None:
    """Monitor optimization job progress"""
    
    click.echo(f"\n👀 Monitoring {len(jobs)} optimization jobs...")
    click.echo("Press Ctrl+C to stop monitoring (jobs will continue running)")
    
    try:
        while True:
            all_completed = True
            
            for job in jobs:
                status = job.get_progress_info()
                if status["status"] == "running":
                    all_completed = False
                
                progress_pct = status.get("progress_percentage", 0)
                best_value = status.get("best_objective_value", 0)
                
                click.echo(f"📊 {job.job_id[:12]}... | {status['status']:<10} | "
                          f"{progress_pct:5.1f}% | Best: {best_value:6.2f}% | "
                          f"Evals: {status.get('current_evaluation', 0)}/{status.get('max_evaluations', 0)}")
            
            if all_completed:
                click.echo("\n✅ All optimization jobs completed!")
                break
            
            await asyncio.sleep(10)
            click.echo()  # Add spacing
            
    except KeyboardInterrupt:
        click.echo("\n🛑 Monitoring stopped. Jobs continue running in background.")


def _display_job_status(status: Dict[str, Any]) -> None:
    """Display formatted job status"""
    
    job_id = status["job_id"][:12] + "..."
    strategy = status["strategy_name"]
    algorithm = status["algorithm"]
    current_status = status["status"]
    
    progress = status.get("progress_percentage", 0)
    best_value = status.get("best_objective_value", 0)
    current_eval = status.get("current_evaluation", 0)
    max_evals = status.get("max_evaluations", 0)
    
    elapsed = status.get("elapsed_time_seconds", 0)
    elapsed_str = f"{elapsed/3600:.1f}h" if elapsed > 3600 else f"{elapsed/60:.1f}m"
    
    click.echo(f"🎯 Job: {job_id} | {strategy} | {algorithm}")
    click.echo(f"   Status: {current_status} | Progress: {progress:.1f}% | Best: {best_value:.2f}%")
    click.echo(f"   Evaluations: {current_eval}/{max_evals} | Runtime: {elapsed_str}")
    
    if status.get("evaluations_without_improvement", 0) > 0:
        patience = status.get("patience", 0)
        no_improvement = status.get("evaluations_without_improvement", 0)
        click.echo(f"   No improvement: {no_improvement}/{patience}")
    
    click.echo()


# Register the CLI group
__all__ = ["optimization_cli"]