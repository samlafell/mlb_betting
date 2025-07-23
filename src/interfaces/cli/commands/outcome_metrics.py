"""
Outcome Metrics CLI Commands

Commands for managing outcome metrics and data quality impact analysis.
Provides tools to record strategy performance, analyze data quality correlation,
and monitor prediction accuracy.

Reference: /sc:improve - Add outcome metrics to data quality monitoring system
"""

import asyncio
import json
from datetime import datetime, timedelta
from typing import Optional

import click
from rich.console import Console
from rich.panel import Panel
from rich.table import Table
from rich.progress import Progress, SpinnerColumn, TextColumn

from ....core.config import get_settings
from ....core.logging import get_logger, LogComponent
from ....services.monitoring.outcome_metrics_service import OutcomeMetricsService
from ....services.game_outcome_service import GameOutcomeService

logger = get_logger(__name__, LogComponent.CORE)
console = Console()


@click.group(name="outcome-metrics")
def outcome_metrics_group():
    """
    Outcome Metrics Management
    
    Tools for recording and analyzing outcome metrics to understand the
    relationship between data quality and prediction accuracy.
    """
    pass


@outcome_metrics_group.command("record-strategy")
@click.option("--game-id", required=True, help="External game ID")
@click.option("--strategy", required=True, help="Strategy name")
@click.option("--predicted", required=True, type=click.Choice(['win', 'loss', 'over', 'under']), help="Predicted outcome")
@click.option("--actual", required=True, type=click.Choice(['win', 'loss', 'over', 'under']), help="Actual outcome")
@click.option("--confidence", type=float, default=0.8, help="Confidence level (0.0-1.0)")
@click.option("--bet-type", default="moneyline", help="Bet type (moneyline, spread, total)")
def record_strategy_performance(game_id: str, strategy: str, predicted: str, actual: str, confidence: float, bet_type: str):
    """Record strategy performance metric."""
    asyncio.run(_record_strategy_performance_async(game_id, strategy, predicted, actual, confidence, bet_type))


async def _record_strategy_performance_async(game_id: str, strategy: str, predicted: str, actual: str, confidence: float, bet_type: str):
    """Async implementation of record strategy performance."""
    try:
        console.print(f"[blue]Recording strategy performance metric...[/blue]")
        
        service = OutcomeMetricsService()
        
        with Progress(
            SpinnerColumn(),
            TextColumn("[progress.description]{task.description}"),
            console=console
        ) as progress:
            task = progress.add_task("Recording metric...", total=None)
            
            metric_id = await service.record_strategy_performance_metric(
                game_external_id=game_id,
                strategy_name=strategy,
                predicted_outcome=predicted,
                actual_outcome=actual,
                confidence_level=confidence,
                bet_type=bet_type
            )
            
            progress.update(task, completed=True)
        
        console.print(f"[green]✅ Strategy performance metric recorded[/green]")
        console.print(f"Metric ID: {metric_id}")
        console.print(f"Strategy: {strategy}")
        console.print(f"Accuracy: {'✅ Correct' if predicted == actual else '❌ Incorrect'}")
        console.print(f"Confidence: {confidence:.2f}")
        
    except Exception as e:
        console.print(f"[red]❌ Failed to record strategy performance: {e}[/red]")
        logger.error(f"Strategy performance recording error: {e}")
        raise click.ClickException(str(e))


@outcome_metrics_group.command("record-line")
@click.option("--game-id", required=True, help="External game ID")
@click.option("--sportsbook", required=True, help="Sportsbook name")
@click.option("--bet-type", required=True, type=click.Choice(['spread', 'total']), help="Bet type")
@click.option("--opening", type=float, required=True, help="Opening line")
@click.option("--closing", type=float, required=True, help="Closing line")
@click.option("--actual", type=float, required=True, help="Actual game result")
def record_line_accuracy(game_id: str, sportsbook: str, bet_type: str, opening: float, closing: float, actual: float):
    """Record betting line accuracy metric."""
    asyncio.run(_record_line_accuracy_async(game_id, sportsbook, bet_type, opening, closing, actual))


async def _record_line_accuracy_async(game_id: str, sportsbook: str, bet_type: str, opening: float, closing: float, actual: float):
    """Async implementation of record line accuracy."""
    try:
        console.print(f"[blue]Recording betting line accuracy metric...[/blue]")
        
        service = OutcomeMetricsService()
        
        with Progress(
            SpinnerColumn(),
            TextColumn("[progress.description]{task.description}"),
            console=console
        ) as progress:
            task = progress.add_task("Recording metric...", total=None)
            
            metric_id = await service.record_betting_line_accuracy_metric(
                game_external_id=game_id,
                sportsbook_name=sportsbook,
                bet_type=bet_type,
                opening_line=opening,
                closing_line=closing,
                actual_margin=actual
            )
            
            progress.update(task, completed=True)
        
        console.print(f"[green]✅ Betting line accuracy metric recorded[/green]")
        console.print(f"Metric ID: {metric_id}")
        console.print(f"Sportsbook: {sportsbook}")
        console.print(f"Opening Line: {opening}")
        console.print(f"Closing Line: {closing}")
        console.print(f"Actual Result: {actual}")
        console.print(f"Line Movement: {abs(closing - opening):.2f}")
        
    except Exception as e:
        console.print(f"[red]❌ Failed to record line accuracy: {e}[/red]")
        logger.error(f"Line accuracy recording error: {e}")
        raise click.ClickException(str(e))


@outcome_metrics_group.command("strategy-summary")
@click.option("--strategy", required=True, help="Strategy name to analyze")
@click.option("--days", type=int, default=30, help="Number of days to analyze (default: 30)")
@click.option("--json-output", is_flag=True, help="Output as JSON")
def strategy_performance_summary(strategy: str, days: int, json_output: bool):
    """Get strategy performance summary."""
    asyncio.run(_strategy_performance_summary_async(strategy, days, json_output))


async def _strategy_performance_summary_async(strategy: str, days: int, json_output: bool):
    """Async implementation of strategy performance summary."""
    try:
        console.print(f"[blue]Analyzing strategy performance: {strategy}[/blue]")
        
        service = OutcomeMetricsService()
        
        with Progress(
            SpinnerColumn(),
            TextColumn("[progress.description]{task.description}"),
            console=console
        ) as progress:
            task = progress.add_task("Analyzing performance...", total=None)
            
            summary = await service.get_strategy_performance_summary(strategy, days)
            
            progress.update(task, completed=True)
        
        if json_output:
            console.print(json.dumps(summary, indent=2, default=str))
            return
        
        # Display formatted summary
        console.print(f"\n[bold green]Strategy Performance Summary: {strategy}[/bold green]")
        console.print(f"Analysis Period: {days} days")
        
        # Create performance table
        perf_table = Table(title="Overall Performance")
        perf_table.add_column("Metric", style="cyan")
        perf_table.add_column("Value", style="magenta")
        
        perf_table.add_row("Total Predictions", str(summary["total_predictions"]))
        perf_table.add_row("Correct Predictions", str(summary["correct_predictions"]))
        perf_table.add_row("Accuracy Rate", f"{summary['accuracy_rate']:.1%}")
        perf_table.add_row("Average Confidence", f"{summary['average_confidence']:.1%}")
        perf_table.add_row("Average Data Quality", f"{summary['average_data_quality']:.1%}")
        
        console.print(perf_table)
        
        # Create quality impact table
        quality_table = Table(title="Data Quality Impact Analysis")
        quality_table.add_column("Quality Level", style="cyan")
        quality_table.add_column("Accuracy", style="magenta")
        quality_table.add_column("Impact", style="yellow")
        
        quality_table.add_row(
            "High Quality (>80%)", 
            f"{summary['high_quality_accuracy']:.1%}",
            "📈 Best performance"
        )
        quality_table.add_row(
            "Low Quality (<50%)", 
            f"{summary['low_quality_accuracy']:.1%}",
            "📉 Worst performance"
        )
        quality_table.add_row(
            "Sharp Action Present",
            f"{summary['sharp_action_accuracy']:.1%}",
            "⚡ Sharp correlation"
        )
        quality_table.add_row(
            "No Sharp Action",
            f"{summary['no_sharp_accuracy']:.1%}",
            "🔍 Regular patterns"
        )
        
        console.print(quality_table)
        
        # Quality impact assessment
        quality_impact = summary["quality_impact"]
        if quality_impact > 0.1:
            impact_msg = f"[green]🎯 Strong positive impact: +{quality_impact:.1%}[/green]"
        elif quality_impact > 0.05:
            impact_msg = f"[yellow]📊 Moderate positive impact: +{quality_impact:.1%}[/yellow]"
        elif quality_impact > -0.05:
            impact_msg = f"[blue]📋 Neutral impact: {quality_impact:.1%}[/blue]"
        else:
            impact_msg = f"[red]⚠️  Negative impact: {quality_impact:.1%}[/red]"
        
        console.print(f"\n[bold]Data Quality Impact:[/bold] {impact_msg}")
        
    except Exception as e:
        console.print(f"[red]❌ Failed to get strategy summary: {e}[/red]")
        logger.error(f"Strategy summary error: {e}")
        raise click.ClickException(str(e))


@outcome_metrics_group.command("quality-impact")
@click.option("--days", type=int, default=30, help="Number of days to analyze (default: 30)")
@click.option("--json-output", is_flag=True, help="Output as JSON")
def data_quality_impact_analysis(days: int, json_output: bool):
    """Analyze overall data quality impact on prediction accuracy."""
    asyncio.run(_data_quality_impact_analysis_async(days, json_output))


async def _data_quality_impact_analysis_async(days: int, json_output: bool):
    """Async implementation of data quality impact analysis."""
    try:
        console.print(f"[blue]Analyzing data quality impact across all strategies...[/blue]")
        
        service = OutcomeMetricsService()
        
        with Progress(
            SpinnerColumn(),
            TextColumn("[progress.description]{task.description}"),
            console=console
        ) as progress:
            task = progress.add_task("Analyzing quality impact...", total=None)
            
            analysis = await service.get_data_quality_impact_analysis(days)
            
            progress.update(task, completed=True)
        
        if json_output:
            console.print(json.dumps(analysis, indent=2, default=str))
            return
        
        # Display formatted analysis
        console.print(f"\n[bold green]Data Quality Impact Analysis[/bold green]")
        console.print(f"Analysis Period: {days} days")
        
        # Create quality tiers table
        tiers_table = Table(title="Performance by Data Quality Tier")
        tiers_table.add_column("Quality Tier", style="cyan")
        tiers_table.add_column("Predictions", justify="right", style="blue")
        tiers_table.add_column("Avg Accuracy", justify="right", style="magenta")
        tiers_table.add_column("Avg Confidence", justify="right", style="yellow")
        
        quality_tiers = analysis["quality_tiers"]
        tier_order = ["excellent", "good", "fair", "poor"]
        
        for tier in tier_order:
            if tier in quality_tiers:
                data = quality_tiers[tier]
                tiers_table.add_row(
                    tier.title(),
                    str(data["predictions"]),
                    f"{data['avg_accuracy']:.1%}",
                    f"{data['avg_confidence']:.1%}"
                )
        
        console.print(tiers_table)
        
        # Correlation analysis
        corr_coeff = analysis["correlation_coefficient"]
        if corr_coeff > 0.5:
            corr_msg = f"[green]🔗 Strong positive correlation: {corr_coeff:.3f}[/green]"
        elif corr_coeff > 0.3:
            corr_msg = f"[yellow]📈 Moderate positive correlation: {corr_coeff:.3f}[/yellow]"
        elif corr_coeff > 0.1:
            corr_msg = f"[blue]📊 Weak positive correlation: {corr_coeff:.3f}[/blue]"
        elif corr_coeff > -0.1:
            corr_msg = f"[white]➖ No significant correlation: {corr_coeff:.3f}[/white]"
        else:
            corr_msg = f"[red]📉 Negative correlation: {corr_coeff:.3f}[/red]"
        
        console.print(f"\n[bold]Quality-Accuracy Correlation:[/bold] {corr_msg}")
        
        # Impact summary
        impact_summary = analysis["quality_impact_summary"]
        console.print(f"\n[bold]Impact Summary:[/bold]")
        console.print(f"  • Excellent vs Poor: {impact_summary['excellent_vs_poor_diff']:.1%} difference")
        console.print(f"  • Good vs Fair: {impact_summary['good_vs_fair_diff']:.1%} difference")
        
    except Exception as e:
        console.print(f"[red]❌ Failed to analyze quality impact: {e}[/red]")
        logger.error(f"Quality impact analysis error: {e}")
        raise click.ClickException(str(e))


@outcome_metrics_group.command("update-aggregations")
@click.option("--period", type=click.Choice(['daily', 'weekly', 'monthly']), default='daily', help="Aggregation period")
def update_aggregations(period: str):
    """Update outcome metrics aggregations."""
    asyncio.run(_update_aggregations_async(period))


async def _update_aggregations_async(period: str):
    """Async implementation of update aggregations."""
    try:
        console.print(f"[blue]Updating {period} aggregations...[/blue]")
        
        from ....data.database.connection import get_connection
        
        with Progress(
            SpinnerColumn(),
            TextColumn("[progress.description]{task.description}"),
            console=console
        ) as progress:
            task = progress.add_task("Updating aggregations...", total=None)
            
            async with get_connection() as conn:
                result = await conn.fetchval(
                    "SELECT monitoring.update_outcome_metrics_aggregations($1)",
                    period
                )
            
            progress.update(task, completed=True)
        
        console.print(f"[green]✅ Updated {result} {period} aggregations[/green]")
        
    except Exception as e:
        console.print(f"[red]❌ Failed to update aggregations: {e}[/red]")
        logger.error(f"Aggregations update error: {e}")
        raise click.ClickException(str(e))


@outcome_metrics_group.command("migrate")
@click.option("--dry-run", is_flag=True, help="Show migration plan without executing")
def migrate_outcome_metrics(dry_run: bool):
    """Create outcome metrics database tables."""
    asyncio.run(_migrate_outcome_metrics_async(dry_run))


async def _migrate_outcome_metrics_async(dry_run: bool):
    """Async implementation of migrate outcome metrics."""
    try:
        console.print("[blue]Outcome Metrics Database Migration[/blue]")
        
        if dry_run:
            console.print("[yellow]DRY RUN MODE - No changes will be made[/yellow]")
            console.print("\n[green]Migration Plan:[/green]")
            console.print("1. Create monitoring schema")
            console.print("2. Create outcome_metrics table")
            console.print("3. Create outcome_metrics_aggregations table")
            console.print("4. Create indexes for performance")
            console.print("5. Create views for analysis")
            console.print("6. Create aggregation functions")
            console.print("7. Grant permissions")
            console.print("8. Insert sample data")
            return
        
        from ....data.database.connection import get_connection
        
        with Progress(
            SpinnerColumn(),
            TextColumn("[progress.description]{task.description}"),
            console=console
        ) as progress:
            task = progress.add_task("Running migration...", total=None)
            
            # Read and execute migration file
            migration_file = "/Users/samlafell/Documents/programming_projects/mlb_betting_program/sql/migrations/006_create_outcome_metrics_tables.sql"
            
            with open(migration_file, 'r') as f:
                migration_sql = f.read()
            
            async with get_connection() as conn:
                await conn.execute(migration_sql)
            
            progress.update(task, completed=True)
        
        console.print("[green]✅ Outcome metrics migration completed successfully[/green]")
        console.print("\n[blue]Created Components:[/blue]")
        console.print("  • monitoring.outcome_metrics table")
        console.print("  • monitoring.outcome_metrics_aggregations table")
        console.print("  • Performance indexes")
        console.print("  • Analysis views")
        console.print("  • Aggregation functions")
        
    except FileNotFoundError:
        console.print("[red]❌ Migration file not found[/red]")
        raise click.ClickException("Migration file not found")
    except Exception as e:
        console.print(f"[red]❌ Migration failed: {e}[/red]")
        logger.error(f"Migration error: {e}")
        raise click.ClickException(str(e))


# Make the command group available for import
outcome_metrics = outcome_metrics_group