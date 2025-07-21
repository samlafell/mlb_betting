"""
Pipeline Management CLI Commands

Commands for managing the RAW → STAGING → CURATED data pipeline.
Provides pipeline execution, monitoring, and status management.

Reference: docs/SYSTEM_DESIGN_ANALYSIS.md
"""

import asyncio
import json
from datetime import datetime, timedelta
from typing import Any, Dict, Optional
from uuid import UUID

import click
from rich.console import Console
from rich.panel import Panel
from rich.table import Table
from rich.progress import Progress, SpinnerColumn, TextColumn, BarColumn, TimeRemainingColumn

from ....core.config import get_settings
from ....core.logging import get_logger, LogComponent
from ....data.pipeline.pipeline_orchestrator import (
    DataPipelineOrchestrator,
    PipelineMode,
    create_pipeline_orchestrator
)
from ....data.pipeline.zone_interface import (
    ZoneType,
    DataRecord,
    ProcessingStatus
)
from ....data.pipeline.raw_zone_adapter import create_raw_zone_adapter

logger = get_logger(__name__, LogComponent.CORE)
console = Console()


@click.group(name="pipeline")
def pipeline_group():
    """
    Pipeline Management Commands
    
    Manage the RAW → STAGING → CURATED data pipeline for MLB betting data.
    """
    pass


@pipeline_group.command("run")
@click.option("--zone", type=click.Choice(['raw', 'staging', 'curated', 'all']), default='all',
              help="Zone to process (default: all)")
@click.option("--mode", type=click.Choice(['full', 'raw_only', 'staging_only', 'curated_only']), 
              default='full', help="Pipeline execution mode")
@click.option("--source", type=click.Choice(['action_network', 'sbd', 'vsin', 'mlb_stats_api']),
              help="Data source to process")
@click.option("--batch-size", type=int, default=1000, help="Batch size for processing")
@click.option("--dry-run", is_flag=True, help="Show what would be processed without executing")
async def run_pipeline(zone: str, mode: str, source: Optional[str], batch_size: int, dry_run: bool):
    """
    Run the data pipeline for processing betting data.
    
    Examples:
    \b
        # Run full pipeline (all zones)
        uv run -m src.interfaces.cli pipeline run --zone all
        
        # Process only RAW zone
        uv run -m src.interfaces.cli pipeline run --zone raw
        
        # Process specific source through full pipeline
        uv run -m src.interfaces.cli pipeline run --source action_network
        
        # Dry run to see what would be processed
        uv run -m src.interfaces.cli pipeline run --dry-run
    """
    try:
        if dry_run:
            console.print("[yellow]DRY RUN MODE - No actual processing will occur[/yellow]")
        
        console.print(f"[blue]Starting pipeline execution...[/blue]")
        console.print(f"Zone: {zone}, Mode: {mode}, Source: {source or 'all'}")
        
        # Create pipeline orchestrator
        orchestrator = await create_pipeline_orchestrator()
        
        # Get sample records (in real implementation, would query from sources)
        records = await _get_sample_records(source, batch_size)
        
        if not records:
            console.print("[yellow]No records found to process[/yellow]")
            return
        
        console.print(f"[green]Found {len(records)} records to process[/green]")
        
        if dry_run:
            _display_dry_run_summary(records, zone, mode)
            return
        
        # Execute pipeline
        with Progress(
            SpinnerColumn(),
            TextColumn("[progress.description]{task.description}"),
            BarColumn(),
            TimeRemainingColumn(),
            console=console
        ) as progress:
            task = progress.add_task("Processing pipeline...", total=None)
            
            if zone == 'all' or mode == 'full':
                execution = await orchestrator.run_full_pipeline(records, {
                    'source': source,
                    'batch_size': batch_size,
                    'cli_initiated': True
                })
            else:
                zone_type = ZoneType(zone)
                execution = await orchestrator.run_zone_pipeline(zone_type, records, {
                    'source': source,
                    'batch_size': batch_size,
                    'cli_initiated': True
                })
            
            progress.update(task, completed=True)
        
        # Display results
        _display_execution_results(execution)
        
        # Cleanup
        await orchestrator.cleanup()
        
    except Exception as e:
        console.print(f"[red]Pipeline execution failed: {e}[/red]")
        logger.error(f"Pipeline execution error: {e}")
        raise click.ClickException(str(e))


@pipeline_group.command("status")
@click.option("--zone", type=click.Choice(['raw', 'staging', 'curated', 'all']), default='all',
              help="Zone status to check")
@click.option("--detailed", is_flag=True, help="Show detailed status information")
@click.option("--execution-id", help="Check specific execution status")
async def pipeline_status(zone: str, detailed: bool, execution_id: Optional[str]):
    """
    Check pipeline status and health.
    
    Examples:
    \b
        # Check overall pipeline status
        uv run -m src.interfaces.cli pipeline status
        
        # Check detailed status for all zones
        uv run -m src.interfaces.cli pipeline status --detailed
        
        # Check specific zone status
        uv run -m src.interfaces.cli pipeline status --zone raw
        
        # Check specific execution status
        uv run -m src.interfaces.cli pipeline status --execution-id <uuid>
    """
    try:
        console.print("[blue]Checking pipeline status...[/blue]")
        
        # Create pipeline orchestrator
        orchestrator = await create_pipeline_orchestrator()
        
        if execution_id:
            # Check specific execution
            execution = await orchestrator.get_execution_status(UUID(execution_id))
            if execution:
                _display_execution_status(execution)
            else:
                console.print(f"[yellow]Execution {execution_id} not found[/yellow]")
            return
        
        # Check zone health
        health_status = await orchestrator.get_zone_health()
        
        if zone == 'all':
            _display_all_zones_status(health_status, detailed)
        else:
            zone_type = ZoneType(zone)
            if zone_type in health_status:
                _display_zone_status(zone_type, health_status[zone_type], detailed)
            else:
                console.print(f"[yellow]Zone {zone} not available or not enabled[/yellow]")
        
        # Show active executions
        active_executions = await orchestrator.list_active_executions()
        if active_executions:
            console.print(f"\n[blue]Active Executions: {len(active_executions)}[/blue]")
            for execution in active_executions:
                console.print(f"  • {execution.execution_id} - {execution.status} - {execution.current_stage}")
        
        await orchestrator.cleanup()
        
    except Exception as e:
        console.print(f"[red]Status check failed: {e}[/red]")
        logger.error(f"Pipeline status error: {e}")
        raise click.ClickException(str(e))


@pipeline_group.command("migrate")
@click.option("--create-schemas", is_flag=True, help="Create pipeline schemas")
@click.option("--migrate-data", is_flag=True, help="Migrate existing data to RAW zone")
@click.option("--source-table", help="Source table to migrate from")
@click.option("--dry-run", is_flag=True, help="Show migration plan without executing")
async def migrate_pipeline(create_schemas: bool, migrate_data: bool, source_table: Optional[str], dry_run: bool):
    """
    Migrate existing system to use pipeline zones.
    
    Examples:
    \b
        # Create pipeline schemas
        uv run -m src.interfaces.cli pipeline migrate --create-schemas
        
        # Migrate existing data to RAW zone
        uv run -m src.interfaces.cli pipeline migrate --migrate-data --source-table core_betting.spreads
        
        # Dry run migration
        uv run -m src.interfaces.cli pipeline migrate --migrate-data --dry-run
    """
    try:
        console.print("[blue]Starting pipeline migration...[/blue]")
        
        if create_schemas:
            console.print("[yellow]Schema creation should be done via SQL migrations[/yellow]")
            console.print("Run: `psql -f sql/migrations/004_create_pipeline_zones.sql`")
        
        if migrate_data:
            console.print(f"[blue]Migrating data from existing tables...[/blue]")
            if dry_run:
                console.print("[yellow]DRY RUN MODE - No actual migration will occur[/yellow]")
            
            # Show migration plan
            console.print("\n[green]Migration Plan:[/green]")
            console.print("1. core_betting.spreads → raw_data.spreads_raw")
            console.print("2. core_betting.totals → raw_data.totals_raw")
            console.print("3. core_betting.moneylines → raw_data.moneylines_raw")
            console.print("\nThis will preserve existing data while enabling the new pipeline.")
            
            if not dry_run:
                console.print("\n[red]Actual migration should be done via SQL scripts[/red]")
                console.print("This ensures data integrity and allows for rollback if needed.")
        
        console.print("[green]Migration planning completed[/green]")
        
    except Exception as e:
        console.print(f"[red]Migration failed: {e}[/red]")
        logger.error(f"Pipeline migration error: {e}")
        raise click.ClickException(str(e))


async def _get_sample_records(source: Optional[str], limit: int) -> list[DataRecord]:
    """Get sample records for processing (placeholder implementation)."""
    # In real implementation, this would query the database or external APIs
    # For now, return sample records
    
    sample_records = []
    
    for i in range(min(limit, 5)):  # Limit to 5 for demo
        record = DataRecord(
            external_id=f"sample_{source or 'generic'}_{i}",
            source=source or 'generic',
            raw_data={
                'game_id': f'game_{i}',
                'home_team': 'Sample Home Team',
                'away_team': 'Sample Away Team',
                'sportsbook': 'Sample Sportsbook',
                'bet_type': 'moneyline',
                'odds': -110
            },
            created_at=datetime.now()
        )
        sample_records.append(record)
    
    return sample_records


def _display_dry_run_summary(records: list[DataRecord], zone: str, mode: str):
    """Display dry run summary."""
    table = Table(title="Dry Run Summary")
    table.add_column("Records", justify="right", style="cyan")
    table.add_column("Zone", style="magenta")
    table.add_column("Mode", style="green")
    table.add_column("Sources", style="yellow")
    
    sources = set(record.source for record in records)
    
    table.add_row(
        str(len(records)),
        zone,
        mode,
        ", ".join(sources)
    )
    
    console.print(table)


def _display_execution_results(execution):
    """Display pipeline execution results."""
    console.print("\n" + "="*60)
    console.print(f"[green]Pipeline Execution Results[/green]")
    console.print("="*60)
    
    # Execution overview
    console.print(f"Execution ID: {execution.execution_id}")
    console.print(f"Status: [{'green' if execution.status == ProcessingStatus.COMPLETED else 'red'}]{execution.status}[/]")
    console.print(f"Mode: {execution.pipeline_mode}")
    console.print(f"Duration: {(execution.end_time - execution.start_time).total_seconds():.2f} seconds")
    
    # Metrics
    metrics = execution.metrics
    console.print(f"\n[blue]Overall Metrics:[/blue]")
    console.print(f"  Total Records: {metrics.total_records}")
    console.print(f"  Successful: {metrics.successful_records}")
    console.print(f"  Failed: {metrics.failed_records}")
    console.print(f"  Success Rate: {(metrics.successful_records/max(metrics.total_records,1)*100):.1f}%")
    
    # Zone-specific results
    if metrics.zone_metrics:
        console.print(f"\n[blue]Zone Results:[/blue]")
        for zone_type, zone_metrics in metrics.zone_metrics.items():
            if isinstance(zone_metrics, dict):
                console.print(f"  {zone_type}:")
                console.print(f"    Processed: {zone_metrics.get('records_processed', 0)}")
                console.print(f"    Successful: {zone_metrics.get('records_successful', 0)}")
                console.print(f"    Failed: {zone_metrics.get('records_failed', 0)}")
    
    # Errors
    if execution.errors:
        console.print(f"\n[red]Errors:[/red]")
        for error in execution.errors[:5]:  # Show first 5 errors
            console.print(f"  • {error}")
        if len(execution.errors) > 5:
            console.print(f"  ... and {len(execution.errors) - 5} more errors")


def _display_execution_status(execution):
    """Display specific execution status."""
    console.print(Panel(
        f"[bold]Execution {execution.execution_id}[/bold]\n\n"
        f"Status: [{'green' if execution.status == ProcessingStatus.COMPLETED else 'yellow'}]{execution.status}[/]\n"
        f"Stage: {execution.current_stage}\n"
        f"Mode: {execution.pipeline_mode}\n"
        f"Started: {execution.start_time.strftime('%Y-%m-%d %H:%M:%S')}\n"
        f"Records: {execution.metrics.total_records}",
        title="Execution Status"
    ))


def _display_all_zones_status(health_status: Dict[ZoneType, Dict[str, Any]], detailed: bool):
    """Display status for all zones."""
    table = Table(title="Pipeline Zones Status")
    table.add_column("Zone", style="cyan")
    table.add_column("Status", style="magenta")
    table.add_column("Records Processed", justify="right", style="green")
    table.add_column("Quality Score", justify="right", style="yellow")
    
    if detailed:
        table.add_column("Error Rate", justify="right", style="red")
    
    for zone_type, health in health_status.items():
        status = health.get('status', 'unknown')
        metrics = health.get('metrics', {})
        
        status_style = "green" if status == "healthy" else "red" if status == "error" else "yellow"
        
        row = [
            zone_type.value,
            f"[{status_style}]{status}[/]",
            str(metrics.get('records_processed', 0)),
            f"{metrics.get('quality_score', 0.0):.2f}"
        ]
        
        if detailed:
            row.append(f"{metrics.get('error_rate', 0.0):.2f}%")
        
        table.add_row(*row)
    
    console.print(table)


def _display_zone_status(zone_type: ZoneType, health: Dict[str, Any], detailed: bool):
    """Display status for specific zone."""
    status = health.get('status', 'unknown')
    metrics = health.get('metrics', {})
    
    console.print(Panel(
        f"[bold]{zone_type.value.upper()} Zone Status[/bold]\n\n"
        f"Status: [{'green' if status == 'healthy' else 'red'}]{status}[/]\n"
        f"Records Processed: {metrics.get('records_processed', 0)}\n"
        f"Successful: {metrics.get('records_successful', 0)}\n"
        f"Failed: {metrics.get('records_failed', 0)}\n"
        f"Quality Score: {metrics.get('quality_score', 0.0):.2f}\n"
        f"Error Rate: {metrics.get('error_rate', 0.0):.2f}%",
        title=f"{zone_type.value.title()} Zone"
    ))
    
    if detailed and 'error' in health:
        console.print(f"[red]Error: {health['error']}[/red]")


# Make the command available for import
pipeline = pipeline_group