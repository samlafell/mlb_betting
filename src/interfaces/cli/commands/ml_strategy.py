"""
CLI Commands for ML Strategy Development and Management

This module provides comprehensive CLI commands for managing the complete
ML strategy development lifecycle, from initial development through
production deployment and ongoing optimization.
"""

import asyncio
import json
from datetime import datetime, timedelta
from pathlib import Path

import click
from rich.console import Console
from rich.table import Table
from rich.progress import Progress, SpinnerColumn, TextColumn
from rich.panel import Panel
from rich.tree import Tree

from src.core.config import get_settings
from src.core.logging import LogComponent, get_logger
from src.analysis.strategy_development_framework import (
    StrategyConfiguration, StrategyType, ValidationMethod, strategy_framework
)
from src.analysis.validation.integrated_validation_engine import (
    validation_engine, ValidationPhase, CrossValidationConfig
)
from src.analysis.testing.ab_testing_framework import (
    ab_testing_engine, ExperimentArm, TestType
)
from src.ml.registry.betting_model_registry import (
    betting_model_registry, BettingModelStage
)
from src.analysis.orchestration.strategy_orchestrator import (
    strategy_orchestrator, WorkflowStage, OrchestrationMode
)

console = Console()
logger = get_logger(__name__, LogComponent.CLI)


@click.group(name="ml-strategy")
def ml_strategy_group():
    """ML Strategy Development and Management Commands"""
    pass


@ml_strategy_group.command("create-workflow")
@click.option("--name", "-n", required=True, help="Strategy name")
@click.option("--description", "-d", help="Strategy description")
@click.option("--strategy-type", "-t", 
              type=click.Choice(["rule_based", "ml_predictive", "hybrid", "ensemble"]),
              default="hybrid", help="Strategy type")
@click.option("--validation-method", "-v",
              type=click.Choice(["backtesting_only", "ml_cross_validation", "integrated", "a_b_testing"]),
              default="integrated", help="Validation methodology")
@click.option("--orchestration-mode", "-m",
              type=click.Choice(["manual", "semi_automated", "fully_automated"]),
              default="semi_automated", help="Workflow automation level")
@click.option("--confidence-threshold", "-c", default=0.6, type=float, 
              help="Minimum confidence threshold for recommendations")
@click.option("--max-bet-size", "-b", default=100, type=float, help="Maximum bet size")
@click.option("--processor-type", "-p", default="sharp_action",
              type=click.Choice(["sharp_action", "consensus", "timing_based", "underdog_value", "public_fade"]),
              help="Rule-based processor type")
@click.option("--ml-targets", "-ml", multiple=True, 
              default=["moneyline_home_win"],
              help="ML prediction targets")
def create_strategy_workflow(
    name: str,
    description: str,
    strategy_type: str,
    validation_method: str,
    orchestration_mode: str,
    confidence_threshold: float,
    max_bet_size: float,
    processor_type: str,
    ml_targets: list
):
    """Create new strategy development workflow"""
    asyncio.run(_create_strategy_workflow_async(
        name, description, strategy_type, validation_method, orchestration_mode,
        confidence_threshold, max_bet_size, processor_type, ml_targets
    ))


async def _create_strategy_workflow_async(
    name: str, description: str, strategy_type: str, validation_method: str,
    orchestration_mode: str, confidence_threshold: float, max_bet_size: float,
    processor_type: str, ml_targets: list
):
    """Async implementation of create strategy workflow"""
    try:
        console.print(f"[bold blue]Creating ML Strategy Workflow: {name}[/bold blue]")
        console.print()
        
        # Initialize orchestrator
        with Progress(SpinnerColumn(), TextColumn("[progress.description]{task.description}")) as progress:
            task = progress.add_task("Initializing strategy orchestrator...", total=None)
            
            success = await strategy_orchestrator.initialize()
            if not success:
                console.print("[red]Failed to initialize strategy orchestrator[/red]")
                return
        
        # Create strategy configuration
        strategy_config = StrategyConfiguration(
            strategy_id="",  # Will be set by orchestrator
            name=name,
            description=description or f"ML strategy: {name}",
            strategy_type=StrategyType(strategy_type),
            validation_method=ValidationMethod(validation_method),
            rule_parameters={
                "processor_type": processor_type,
                "min_confidence_threshold": confidence_threshold,
                "enable_debug_logging": False
            },
            ml_prediction_targets=list(ml_targets),
            confidence_threshold=confidence_threshold,
            max_bet_size=max_bet_size
        )
        
        # Create workflow
        workflow_id = await strategy_orchestrator.create_strategy_workflow(
            strategy_config=strategy_config,
            orchestration_mode=OrchestrationMode(orchestration_mode)
        )
        
        console.print(f"[green]✅ Strategy workflow created successfully![/green]")
        console.print(f"[cyan]Workflow ID: {workflow_id}[/cyan]")
        console.print()
        
        # Display workflow details
        _display_strategy_config(strategy_config)
        
        # Show next steps
        console.print("[bold yellow]Next Steps:[/bold yellow]")
        console.print("1. Execute workflow stages: [cyan]uv run -m src.interfaces.cli ml-strategy execute-workflow --workflow-id " + workflow_id + "[/cyan]")
        console.print("2. Monitor progress: [cyan]uv run -m src.interfaces.cli ml-strategy status --workflow-id " + workflow_id + "[/cyan]")
        console.print("3. View all workflows: [cyan]uv run -m src.interfaces.cli ml-strategy list-workflows[/cyan]")
        
    except Exception as e:
        console.print(f"[red]Error creating strategy workflow: {e}[/red]")
        if "--verbose" in str(e):
            console.print_exception()


@ml_strategy_group.command("execute-workflow")
@click.option("--workflow-id", "-w", required=True, help="Workflow ID to execute")
@click.option("--target-stage", "-s",
              type=click.Choice([
                  "development", "validation", "backtesting", "paper_trading",
                  "staging", "a_b_testing", "production", "monitoring"
              ]), help="Target stage to execute (default: next stage)")
@click.option("--full-execution", "-f", is_flag=True, 
              help="Execute complete workflow to production")
@click.option("--force", is_flag=True, help="Skip validation checks")
def execute_workflow(workflow_id: str, target_stage: str, full_execution: bool, force: bool):
    """Execute strategy workflow stage(s)"""
    asyncio.run(_execute_workflow_async(workflow_id, target_stage, full_execution, force))


async def _execute_workflow_async(workflow_id: str, target_stage: str, full_execution: bool, force: bool):
    """Async implementation of execute workflow"""
    try:
        console.print(f"[bold blue]Executing Strategy Workflow: {workflow_id}[/bold blue]")
        console.print()
        
        # Initialize orchestrator
        await strategy_orchestrator.initialize()
        
        if full_execution:
            console.print("[yellow]Executing complete workflow to production...[/yellow]")
            
            with Progress(SpinnerColumn(), TextColumn("[progress.description]{task.description}")) as progress:
                task = progress.add_task("Executing full workflow...", total=None)
                
                success = await strategy_orchestrator.execute_full_workflow(
                    workflow_id=workflow_id,
                    target_stage=WorkflowStage.PRODUCTION
                )
                
                if success:
                    progress.update(task, description="✅ Full workflow completed!")
                else:
                    progress.update(task, description="❌ Workflow execution failed")
        else:
            # Execute single stage
            target_stage_enum = WorkflowStage(target_stage) if target_stage else None
            
            with Progress(SpinnerColumn(), TextColumn("[progress.description]{task.description}")) as progress:
                stage_desc = f"to {target_stage}" if target_stage else "next stage"
                task = progress.add_task(f"Executing workflow stage {stage_desc}...", total=None)
                
                success = await strategy_orchestrator.execute_workflow_stage(
                    workflow_id=workflow_id,
                    target_stage=target_stage_enum,
                    force=force
                )
                
                if success:
                    progress.update(task, description=f"✅ Stage {stage_desc} completed!")
                else:
                    progress.update(task, description=f"❌ Stage {stage_desc} failed")
        
        # Display updated status
        status = await strategy_orchestrator.get_workflow_status(workflow_id)
        _display_workflow_status(status)
        
    except Exception as e:
        console.print(f"[red]Error executing workflow: {e}[/red]")


@ml_strategy_group.command("status")
@click.option("--workflow-id", "-w", help="Specific workflow ID to check")
@click.option("--detailed", "-d", is_flag=True, help="Show detailed status")
def workflow_status(workflow_id: str, detailed: bool):
    """Show workflow status"""
    asyncio.run(_workflow_status_async(workflow_id, detailed))


async def _workflow_status_async(workflow_id: str, detailed: bool):
    """Async implementation of workflow status"""
    try:
        await strategy_orchestrator.initialize()
        
        if workflow_id:
            # Show specific workflow status
            status = await strategy_orchestrator.get_workflow_status(workflow_id)
            
            if "error" in status:
                console.print(f"[red]Error: {status['error']}[/red]")
                return
            
            _display_workflow_status(status, detailed=detailed)
        else:
            # Show all workflows summary
            summary = await strategy_orchestrator.get_all_workflows_summary()
            _display_workflows_summary(summary)
        
    except Exception as e:
        console.print(f"[red]Error getting workflow status: {e}[/red]")


@ml_strategy_group.command("list-workflows")
@click.option("--stage", "-s", help="Filter by workflow stage")
@click.option("--status", help="Filter by workflow status")
@click.option("--strategy-type", "-t", help="Filter by strategy type")
def list_workflows(stage: str, status: str, strategy_type: str):
    """List all strategy workflows"""
    asyncio.run(_list_workflows_async(stage, status, strategy_type))


async def _list_workflows_async(stage: str, status: str, strategy_type: str):
    """Async implementation of list workflows"""
    try:
        await strategy_orchestrator.initialize()
        
        summary = await strategy_orchestrator.get_all_workflows_summary()
        
        console.print("[bold blue]Strategy Workflows Summary[/bold blue]")
        console.print()
        
        # Summary statistics
        stats_table = Table(title="📊 Workflow Statistics", show_header=True, header_style="bold magenta")
        stats_table.add_column("Metric", style="cyan")
        stats_table.add_column("Count", style="green")
        
        stats_table.add_row("Total Active Workflows", str(summary["total_active_workflows"]))
        stats_table.add_row("Total Completed Workflows", str(summary["total_completed_workflows"]))
        
        console.print(stats_table)
        console.print()
        
        # Workflows by stage
        if summary["workflows_by_stage"]:
            stage_table = Table(title="📈 Workflows by Stage", show_header=True, header_style="bold magenta")
            stage_table.add_column("Stage", style="cyan")
            stage_table.add_column("Count", style="green")
            
            for stage_name, count in summary["workflows_by_stage"].items():
                stage_table.add_row(stage_name, str(count))
            
            console.print(stage_table)
            console.print()
        
        # Active workflows
        if summary["active_workflows"]:
            workflows_table = Table(title="🔄 Active Workflows", show_header=True, header_style="bold magenta")
            workflows_table.add_column("Workflow ID", style="cyan")
            workflows_table.add_column("Strategy Name", style="green")
            workflows_table.add_column("Current Stage", style="yellow")
            workflows_table.add_column("Status", style="blue")
            workflows_table.add_column("Progress", style="magenta")
            workflows_table.add_column("Last Updated", style="dim")
            
            for workflow in summary["active_workflows"]:
                # Apply filters
                if stage and workflow["current_stage"] != stage:
                    continue
                if status and workflow["status"] != status:
                    continue
                
                progress_str = f"{workflow['progress']:.0%}"
                updated_str = workflow["updated_at"].split("T")[0]  # Just date
                
                workflows_table.add_row(
                    workflow["workflow_id"][:8] + "...",  # Truncate ID
                    workflow["strategy_name"],
                    workflow["current_stage"],
                    workflow["status"],
                    progress_str,
                    updated_str
                )
            
            console.print(workflows_table)
        
    except Exception as e:
        console.print(f"[red]Error listing workflows: {e}[/red]")


@ml_strategy_group.command("validate")
@click.option("--workflow-id", "-w", required=True, help="Workflow ID to validate")
@click.option("--validation-phase", "-p", 
              type=click.Choice(["development", "pre_staging", "staging", "pre_production", "production"]),
              default="development", help="Validation phase")
@click.option("--start-date", "-s", help="Validation start date (YYYY-MM-DD)")
@click.option("--end-date", "-e", help="Validation end date (YYYY-MM-DD)")
@click.option("--cross-validation", "-cv", is_flag=True, help="Run cross-temporal validation")
@click.option("--cv-folds", default=5, help="Number of cross-validation folds")
def validate_strategy(workflow_id: str, validation_phase: str, start_date: str, end_date: str, 
                     cross_validation: bool, cv_folds: int):
    """Run comprehensive strategy validation"""
    asyncio.run(_validate_strategy_async(workflow_id, validation_phase, start_date, end_date, cross_validation, cv_folds))


async def _validate_strategy_async(workflow_id: str, validation_phase: str, start_date: str, end_date: str,
                                   cross_validation: bool, cv_folds: int):
    """Async implementation of validate strategy"""
    try:
        console.print(f"[bold blue]Validating Strategy Workflow: {workflow_id}[/bold blue]")
        console.print()
        
        # Initialize services
        await strategy_orchestrator.initialize()
        await validation_engine.initialize()
        
        # Get workflow and strategy config
        workflow_status = await strategy_orchestrator.get_workflow_status(workflow_id)
        if "error" in workflow_status:
            console.print(f"[red]Error: {workflow_status['error']}[/red]")
            return
        
        # Parse dates
        if start_date:
            start_dt = datetime.strptime(start_date, "%Y-%m-%d")
        else:
            start_dt = datetime.now() - timedelta(days=90)
        
        if end_date:
            end_dt = datetime.strptime(end_date, "%Y-%m-%d")
        else:
            end_dt = datetime.now() - timedelta(days=30)
        
        console.print(f"📅 Validation Period: {start_dt.date()} to {end_dt.date()}")
        console.print(f"🔍 Validation Phase: {validation_phase}")
        console.print()
        
        # Get strategy configuration (mock for now)
        strategy_config = StrategyConfiguration(
            strategy_id=workflow_id,
            name=workflow_status["strategy_name"],
            description="Strategy validation",
            strategy_type=StrategyType(workflow_status["strategy_type"]),
            validation_method=ValidationMethod.INTEGRATED_VALIDATION
        )
        
        if cross_validation:
            # Cross-temporal validation
            console.print("[yellow]Running cross-temporal validation...[/yellow]")
            
            cv_config = CrossValidationConfig(
                n_splits=cv_folds,
                test_size=0.2,
                purging_buffer_days=1,
                embargo_days=1
            )
            
            with Progress(SpinnerColumn(), TextColumn("[progress.description]{task.description}")) as progress:
                task = progress.add_task("Running cross-temporal validation...", total=None)
                
                cv_results = await validation_engine.validate_cross_temporal(
                    strategy_config, start_dt, end_dt, cv_config
                )
                
                progress.update(task, description="✅ Cross-temporal validation completed!")
            
            # Display CV results
            _display_cross_validation_results(cv_results)
        
        else:
            # Comprehensive validation
            with Progress(SpinnerColumn(), TextColumn("[progress.description]{task.description}")) as progress:
                task = progress.add_task("Running comprehensive validation...", total=None)
                
                validation_result = await validation_engine.validate_strategy_comprehensive(
                    strategy_config,
                    ValidationPhase(validation_phase),
                    start_dt,
                    end_dt
                )
                
                progress.update(task, description="✅ Comprehensive validation completed!")
            
            # Display validation results
            _display_validation_results(validation_result)
        
    except Exception as e:
        console.print(f"[red]Error validating strategy: {e}[/red]")


@ml_strategy_group.command("setup-ab-test")
@click.option("--champion-workflow", "-c", required=True, help="Champion workflow ID")
@click.option("--challenger-workflow", "-ch", required=True, help="Challenger workflow ID")
@click.option("--traffic-split", "-t", default=0.8, type=float, help="Traffic to champion (0.0-1.0)")
@click.option("--duration-days", "-d", default=14, type=int, help="Test duration in days")
@click.option("--min-sample-size", "-s", default=100, type=int, help="Minimum sample size per arm")
def setup_ab_test(champion_workflow: str, challenger_workflow: str, traffic_split: float, 
                  duration_days: int, min_sample_size: int):
    """Setup A/B test between two strategies"""
    asyncio.run(_setup_ab_test_async(champion_workflow, challenger_workflow, traffic_split, duration_days, min_sample_size))


async def _setup_ab_test_async(champion_workflow: str, challenger_workflow: str, traffic_split: float,
                               duration_days: int, min_sample_size: int):
    """Async implementation of setup A/B test"""
    try:
        console.print("[bold blue]Setting up A/B Test[/bold blue]")
        console.print()
        
        # Initialize services
        await ab_testing_engine.initialize()
        await strategy_orchestrator.initialize()
        
        # Get workflow details
        champion_status = await strategy_orchestrator.get_workflow_status(champion_workflow)
        challenger_status = await strategy_orchestrator.get_workflow_status(challenger_workflow)
        
        if "error" in champion_status or "error" in challenger_status:
            console.print("[red]Error: Could not retrieve workflow details[/red]")
            return
        
        # Create mock strategy configurations
        champion_config = StrategyConfiguration(
            strategy_id=champion_workflow,
            name=f"Champion: {champion_status['strategy_name']}",
            description="Champion strategy",
            strategy_type=StrategyType(champion_status["strategy_type"]),
            validation_method=ValidationMethod.A_B_TESTING
        )
        
        challenger_config = StrategyConfiguration(
            strategy_id=challenger_workflow,
            name=f"Challenger: {challenger_status['strategy_name']}",
            description="Challenger strategy",
            strategy_type=StrategyType(challenger_status["strategy_type"]),
            validation_method=ValidationMethod.A_B_TESTING
        )
        
        # Create experiment arms
        arms = [
            ExperimentArm(
                arm_id="champion",
                name=champion_config.name,
                strategy_config=champion_config,
                traffic_allocation=traffic_split,
                is_control=True
            ),
            ExperimentArm(
                arm_id="challenger",
                name=challenger_config.name,
                strategy_config=challenger_config,
                traffic_allocation=1.0 - traffic_split,
                is_control=False
            )
        ]
        
        # Create A/B test
        with Progress(SpinnerColumn(), TextColumn("[progress.description]{task.description}")) as progress:
            task = progress.add_task("Creating A/B test experiment...", total=None)
            
            experiment_id = await ab_testing_engine.create_experiment(
                name=f"Champion vs Challenger: {champion_status['strategy_name']} vs {challenger_status['strategy_name']}",
                description=f"A/B test comparing strategies",
                arms=arms,
                test_type=TestType.CHAMPION_CHALLENGER,
                duration_days=duration_days,
                min_sample_size_per_arm=min_sample_size,
                primary_metric="roi",
                secondary_metrics=["win_rate", "profit", "max_drawdown"]
            )
            
            progress.update(task, description="✅ A/B test created successfully!")
        
        console.print(f"[green]✅ A/B Test Setup Complete![/green]")
        console.print(f"[cyan]Experiment ID: {experiment_id}[/cyan]")
        console.print()
        
        # Display test configuration
        config_table = Table(title="🧪 A/B Test Configuration", show_header=True, header_style="bold magenta")
        config_table.add_column("Parameter", style="cyan")
        config_table.add_column("Value", style="green")
        
        config_table.add_row("Champion Strategy", champion_status["strategy_name"])
        config_table.add_row("Challenger Strategy", challenger_status["strategy_name"])
        config_table.add_row("Traffic Split", f"{traffic_split:.1%} / {(1-traffic_split):.1%}")
        config_table.add_row("Duration", f"{duration_days} days")
        config_table.add_row("Min Sample Size", str(min_sample_size))
        config_table.add_row("Primary Metric", "ROI")
        
        console.print(config_table)
        console.print()
        
        console.print("[bold yellow]Next Steps:[/bold yellow]")
        console.print(f"1. Monitor test: [cyan]uv run -m src.interfaces.cli ml-strategy ab-test-status --experiment-id {experiment_id}[/cyan]")
        console.print(f"2. Analyze results: [cyan]uv run -m src.interfaces.cli ml-strategy analyze-ab-test --experiment-id {experiment_id}[/cyan]")
        
    except Exception as e:
        console.print(f"[red]Error setting up A/B test: {e}[/red]")


def _display_strategy_config(config: StrategyConfiguration):
    """Display strategy configuration in formatted table"""
    config_table = Table(title="📋 Strategy Configuration", show_header=True, header_style="bold magenta")
    config_table.add_column("Parameter", style="cyan")
    config_table.add_column("Value", style="green")
    
    config_table.add_row("Name", config.name)
    config_table.add_row("Strategy Type", config.strategy_type.value)
    config_table.add_row("Validation Method", config.validation_method.value)
    config_table.add_row("Confidence Threshold", f"{config.confidence_threshold:.1%}")
    config_table.add_row("Max Bet Size", f"${config.max_bet_size}")
    
    if config.ml_prediction_targets:
        config_table.add_row("ML Targets", ", ".join(config.ml_prediction_targets))
    
    if config.rule_parameters:
        processor_type = config.rule_parameters.get("processor_type", "N/A")
        config_table.add_row("Processor Type", processor_type)
    
    console.print(config_table)
    console.print()


def _display_workflow_status(status: Dict[str, Any], detailed: bool = False):
    """Display workflow status information"""
    
    # Main status panel
    status_text = f"""
[bold cyan]Strategy:[/bold cyan] {status['strategy_name']}
[bold cyan]Type:[/bold cyan] {status['strategy_type']}
[bold cyan]Current Stage:[/bold cyan] {status['current_stage']}
[bold cyan]Status:[/bold cyan] {status['status']}
[bold cyan]Progress:[/bold cyan] {status['progress_percentage']:.0f}%
[bold cyan]Mode:[/bold cyan] {status['orchestration_mode']}
"""
    
    console.print(Panel(status_text.strip(), title="🎯 Workflow Status", border_style="blue"))
    
    # Progress visualization
    completed_stages = status.get('completed_stages', [])
    current_stage = status.get('current_stage', '')
    
    progress_tree = Tree("📈 Workflow Progress")
    
    all_stages = ["ideation", "development", "validation", "backtesting", "paper_trading", 
                  "staging", "a_b_testing", "production", "monitoring"]
    
    for stage in all_stages:
        if stage in completed_stages:
            progress_tree.add(f"✅ {stage.replace('_', ' ').title()}")
        elif stage == current_stage:
            progress_tree.add(f"🔄 {stage.replace('_', ' ').title()} (Current)")
        else:
            progress_tree.add(f"⏳ {stage.replace('_', ' ').title()}")
    
    console.print(progress_tree)
    console.print()
    
    # Next action
    next_action = status.get('next_action', {})
    if next_action:
        action_text = f"""
[bold yellow]Action:[/bold yellow] {next_action.get('action', 'N/A')}
[bold yellow]Description:[/bold yellow] {next_action.get('description', 'N/A')}
[bold yellow]Automated:[/bold yellow] {next_action.get('automated', False)}
"""
        console.print(Panel(action_text.strip(), title="⚡ Next Action", border_style="yellow"))
    
    # Alerts (if any)
    alerts = status.get('alerts', [])
    if alerts:
        console.print("[bold red]🚨 Recent Alerts:[/bold red]")
        for alert in alerts[-3:]:  # Show last 3 alerts
            severity_color = {"error": "red", "warning": "yellow", "info": "blue", "success": "green"}.get(alert.get("severity", "info"), "white")
            console.print(f"  [{severity_color}]{alert.get('type', 'alert')}: {alert.get('message', 'No message')}[/{severity_color}]")
        console.print()
    
    if detailed:
        # Detailed results
        if status.get('recent_results'):
            results = status['recent_results']
            
            if results.get('validation'):
                console.print("[bold green]📊 Latest Validation Results:[/bold green]")
                console.print(f"  Passed: {results['validation'].get('passed', 'N/A')}")
                console.print(f"  Confidence: {results['validation'].get('confidence_score', 'N/A')}")
                console.print()
            
            if results.get('backtesting'):
                console.print("[bold green]📈 Latest Backtesting Results:[/bold green]")
                console.print(f"  ROI: {results['backtesting'].get('roi_percentage', 'N/A')}%")
                console.print(f"  Win Rate: {results['backtesting'].get('win_rate', 'N/A'):.1%}")
                console.print()


def _display_workflows_summary(summary: Dict[str, Any]):
    """Display workflows summary"""
    
    # Statistics
    stats_panel = f"""
[bold cyan]Total Active:[/bold cyan] {summary['total_active_workflows']}
[bold cyan]Total Completed:[/bold cyan] {summary['total_completed_workflows']}
"""
    console.print(Panel(stats_panel.strip(), title="📊 Workflow Statistics", border_style="blue"))
    
    # By stage breakdown
    if summary.get('workflows_by_stage'):
        stage_table = Table(title="📈 Workflows by Stage", show_header=True, header_style="bold magenta")
        stage_table.add_column("Stage", style="cyan")
        stage_table.add_column("Count", style="green")
        
        for stage, count in summary['workflows_by_stage'].items():
            stage_table.add_row(stage.replace('_', ' ').title(), str(count))
        
        console.print(stage_table)


def _display_validation_results(result):
    """Display comprehensive validation results"""
    
    # Main result
    result_color = "green" if result.passed else "red"
    result_text = "PASSED" if result.passed else "FAILED"
    
    console.print(f"[bold {result_color}]Validation Result: {result_text}[/bold {result_color}]")
    console.print(f"[cyan]Confidence Score: {result.confidence_score:.2f}[/cyan]")
    console.print(f"[cyan]Phase: {result.phase.value}[/cyan]")
    console.print()
    
    # Metrics
    if hasattr(result.metrics, '__dict__'):
        metrics_table = Table(title="📊 Performance Metrics", show_header=True, header_style="bold magenta")
        metrics_table.add_column("Metric", style="cyan")
        metrics_table.add_column("Value", style="green")
        
        metrics_dict = result.metrics.__dict__
        for key, value in metrics_dict.items():
            if value is not None:
                if isinstance(value, float):
                    if "percentage" in key or "rate" in key:
                        formatted_value = f"{value:.1%}" if value <= 1 else f"{value:.1f}%"
                    else:
                        formatted_value = f"{value:.3f}"
                else:
                    formatted_value = str(value)
                
                display_key = key.replace('_', ' ').title()
                metrics_table.add_row(display_key, formatted_value)
        
        console.print(metrics_table)
    
    # Recommendations
    if result.recommendations:
        console.print("[bold yellow]💡 Recommendations:[/bold yellow]")
        for rec in result.recommendations:
            console.print(f"  • {rec}")
        console.print()


def _display_cross_validation_results(cv_results: Dict[str, Any]):
    """Display cross-temporal validation results"""
    
    summary = cv_results.get('summary', {})
    
    # Overall result
    passed = cv_results.get('validation_passed', False)
    result_color = "green" if passed else "red"
    result_text = "PASSED" if passed else "FAILED"
    
    console.print(f"[bold {result_color}]Cross-Temporal Validation: {result_text}[/bold {result_color}]")
    console.print()
    
    # Summary metrics
    summary_table = Table(title="📊 Cross-Validation Summary", show_header=True, header_style="bold magenta")
    summary_table.add_column("Metric", style="cyan")
    summary_table.add_column("Value", style="green")
    
    summary_table.add_row("Number of Folds", str(summary.get('n_folds', 'N/A')))
    summary_table.add_row("Mean ROI", f"{summary.get('mean_roi', 0):.2f}%")
    summary_table.add_row("ROI Std Dev", f"{summary.get('std_roi', 0):.2f}%")
    summary_table.add_row("Mean Win Rate", f"{summary.get('mean_win_rate', 0):.1%}")
    summary_table.add_row("ROI Consistency", f"{summary.get('roi_consistency', 0):.2f}")
    
    console.print(summary_table)
    console.print()
    
    # Individual fold results
    fold_results = cv_results.get('fold_results', [])
    if fold_results:
        fold_table = Table(title="📈 Fold Results", show_header=True, header_style="bold magenta")
        fold_table.add_column("Fold", style="cyan")
        fold_table.add_column("ROI", style="green")
        fold_table.add_column("Win Rate", style="yellow")
        fold_table.add_column("Samples", style="blue")
        
        for fold in fold_results:
            metrics = fold.get('backtesting_metrics', {})
            fold_table.add_row(
                str(fold.get('fold', 'N/A')),
                f"{metrics.get('roi', 0):.2f}%",
                f"{metrics.get('win_rate', 0):.1%}",
                str(metrics.get('sample_size', 0))
            )
        
        console.print(fold_table)


if __name__ == "__main__":
    ml_strategy_group()