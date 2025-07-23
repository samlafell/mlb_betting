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
from ....data.pipeline.sbd_staging_processor import SBDStagingProcessor

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
def run_pipeline(zone: str, mode: str, source: Optional[str], batch_size: int, dry_run: bool):
    """Pipeline run command wrapper that runs async function."""
    asyncio.run(_run_pipeline_async(zone, mode, source, batch_size, dry_run))


async def _run_pipeline_async(zone: str, mode: str, source: Optional[str], batch_size: int, dry_run: bool):
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
        
        # Get real records from database (NO MOCK DATA)
        records = await _get_real_records(source, batch_size)
        
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
                execution = await orchestrator.run_single_zone_pipeline(zone_type, records, {
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
def pipeline_status(zone: str, detailed: bool, execution_id: Optional[str]):
    """Status command wrapper that runs async function."""
    asyncio.run(_pipeline_status_async(zone, detailed, execution_id))


async def _pipeline_status_async(zone: str, detailed: bool, execution_id: Optional[str]):
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
def migrate_pipeline(create_schemas: bool, migrate_data: bool, source_table: Optional[str], dry_run: bool):
    """Pipeline migrate command wrapper that runs async function."""
    asyncio.run(_migrate_pipeline_async(create_schemas, migrate_data, source_table, dry_run))


async def _migrate_pipeline_async(create_schemas: bool, migrate_data: bool, source_table: Optional[str], dry_run: bool):
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


async def _get_real_records(source: Optional[str], limit: int) -> list[DataRecord]:
    """Get real records from database for processing (NO MOCK DATA)."""
    from ....data.database.connection import get_connection
    
    real_records = []
    
    try:
        async with get_connection() as conn:
            # Query actual data from raw_data tables that have been processed_at IS NULL
            # This queries real data from the database, not mock/sample data
            
            table_mapping = {
                'action_network': ['action_network_odds', 'action_network_games', 'action_network_history'],
                'sbd': ['sbd_betting_splits'],
                'vsin': ['vsin_data'],
                'mlb_stats_api': ['mlb_stats_api_games', 'mlb_stats_api']
            }
            
            tables_to_query = []
            if source == 'all' or source is None:
                # Query all tables
                for source_tables in table_mapping.values():
                    tables_to_query.extend(source_tables)
            elif source in table_mapping:
                tables_to_query = table_mapping[source]
            else:
                logger.warning(f"Unknown source: {source}")
                return []
            
            # Query each table for unprocessed records
            for table in tables_to_query:
                try:
                    # Get table-specific column names
                    external_id_col = _get_external_id_column(table)
                    
                    query = f"""
                        SELECT id, 
                               {external_id_col} as external_id,
                               collected_at,
                               CASE WHEN created_at IS NOT NULL THEN created_at ELSE collected_at END as created_at
                        FROM raw_data.{table}
                        WHERE processed_at IS NULL
                        ORDER BY collected_at DESC
                        LIMIT $1
                    """
                    
                    rows = await conn.fetch(query, min(limit, 50))
                    
                    for row in rows:
                        # Create DataRecord from real database data
                        record = DataRecord(
                            external_id=row['external_id'] or f"{table}_{row['id']}",
                            source=_get_source_from_table(table),
                            raw_data={
                                'table': table,
                                'id': row['id'],
                                'real_data': True  # Mark as real data
                            },
                            created_at=row['created_at'],
                            collected_at=row['collected_at']
                        )
                        real_records.append(record)
                        
                        if len(real_records) >= limit:
                            break
                            
                except Exception as e:
                    logger.warning(f"Error querying table {table}: {e}")
                    continue
                    
                if len(real_records) >= limit:
                    break
                    
    except Exception as e:
        logger.error(f"Error getting real records: {e}")
        return []
    
    logger.info(f"Retrieved {len(real_records)} real records from database (no mock data)")
    return real_records


def _get_source_from_table(table: str) -> str:
    """Map table name to source name."""
    if 'action_network' in table:
        return 'action_network'
    elif 'sbd' in table:
        return 'sbd' 
    elif 'vsin' in table:
        return 'vsin'
    elif 'mlb_stats_api' in table:
        return 'mlb_stats_api'
    else:
        return 'unknown'


def _get_external_id_column(table: str) -> str:
    """Get the correct external ID column name for each table."""
    # Map table name to its external ID column name
    column_mapping = {
        'action_network_odds': 'external_game_id',
        'action_network_games': 'external_game_id', 
        'action_network_history': 'external_game_id',
        'sbd_betting_splits': 'external_matchup_id',
        'vsin_data': 'external_id',
        'mlb_stats_api_games': 'external_game_id',
        'mlb_stats_api': 'external_id'
    }
    
    return column_mapping.get(table, 'id')  # Fallback to id if unknown table


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


# SBD-Specific Pipeline Commands

@pipeline_group.command("sbd-staging")
@click.option("--limit", type=int, help="Limit number of records to process (for testing)")
@click.option("--dry-run", is_flag=True, help="Show what would be processed without executing")
@click.option("--stats", is_flag=True, help="Show processing statistics")
def sbd_staging_command(limit: Optional[int], dry_run: bool, stats: bool):
    """SBD staging command wrapper that runs async function."""
    asyncio.run(_sbd_staging_async(limit, dry_run, stats))


async def _sbd_staging_async(limit: Optional[int], dry_run: bool, stats: bool):
    """
    Process SBD raw data into staging format.
    
    Transforms complex SBD JSON data into business-readable staging tables.
    
    Examples:
    \b
        # Process all unprocessed SBD records
        uv run -m src.interfaces.cli pipeline sbd-staging
        
        # Test with 10 records
        uv run -m src.interfaces.cli pipeline sbd-staging --limit 10
        
        # Show statistics only
        uv run -m src.interfaces.cli pipeline sbd-staging --stats
        
        # Dry run to see what would be processed
        uv run -m src.interfaces.cli pipeline sbd-staging --dry-run
    """
    try:
        console.print("[blue]SBD Raw-to-Staging Pipeline[/blue]")
        
        # Create SBD staging processor
        from ....data.pipeline.zone_interface import create_zone_config, ZoneType
        settings = get_settings()
        
        config = create_zone_config(ZoneType.STAGING, settings.schemas.staging)
        processor = SBDStagingProcessor(config)
        
        # Show stats if requested
        if stats:
            console.print("[blue]Getting SBD processing statistics...[/blue]")
            stats_data = await processor.get_processing_stats()
            
            # Create stats table
            stats_table = Table(title="SBD Processing Statistics")
            stats_table.add_column("Metric", style="cyan")
            stats_table.add_column("Value", style="magenta")
            
            for key, value in stats_data.items():
                stats_table.add_row(key.replace('_', ' ').title(), str(value))
            
            console.print(stats_table)
            return
        
        # Dry run mode
        if dry_run:
            console.print("[yellow]DRY RUN MODE - No actual processing will occur[/yellow]")
            
            # Count unprocessed records
            from ....data.database.connection import get_connection
            db_connection = get_connection()
            
            async with db_connection.get_async_connection() as connection:
                count_query = """
                    SELECT COUNT(*) as unprocessed
                    FROM raw_data.sbd_betting_splits 
                    WHERE processed_at IS NULL
                """
                if limit:
                    count_query += f" LIMIT {limit}"
                    
                result = await connection.fetchrow(count_query)
                unprocessed_count = result['unprocessed'] if result else 0
                
                console.print(f"[blue]Would process {unprocessed_count} unprocessed SBD records[/blue]")
                if limit:
                    console.print(f"[yellow]Limited to {limit} records for testing[/yellow]")
            
            return
        
        # Process SBD records
        console.print("[blue]Starting SBD staging processing...[/blue]")
        if limit:
            console.print(f"[yellow]Processing limited to {limit} records[/yellow]")
        
        with Progress(
            SpinnerColumn(),
            TextColumn("[progress.description]{task.description}"),
            BarColumn(),
            TimeRemainingColumn(),
            console=console
        ) as progress:
            task = progress.add_task("Processing SBD records...", total=None)
            
            # Run the processor
            result = await processor.process_sbd_raw_records(limit=limit)
            
            progress.update(task, completed=result.records_processed)
        
        # Show results
        if result.status == ProcessingStatus.COMPLETED:
            console.print(f"[green]✅ SBD processing completed successfully[/green]")
            console.print(f"[blue]Records processed: {result.records_processed}[/blue]")
            console.print(f"[green]Records successful: {result.records_successful}[/green]")
            
            if result.records_processed > result.records_successful:
                failed_count = result.records_processed - result.records_successful
                console.print(f"[red]Records failed: {failed_count}[/red]")
                
                if result.errors:
                    console.print("[red]Errors encountered:[/red]")
                    for error in result.errors[:5]:  # Show first 5 errors
                        console.print(f"  • {error}")
                    if len(result.errors) > 5:
                        console.print(f"  ... and {len(result.errors) - 5} more errors")
        else:
            console.print(f"[red]❌ SBD processing failed[/red]")
            if result.errors:
                for error in result.errors:
                    console.print(f"[red]Error: {error}[/red]")
        
        # Show final stats
        console.print("\n[blue]Final Statistics:[/blue]")
        final_stats = await processor.get_processing_stats()
        
        stats_table = Table(title="SBD Processing Results")
        stats_table.add_column("Metric", style="cyan")
        stats_table.add_column("Value", style="magenta")
        
        for key, value in final_stats.items():
            if key != 'error':
                stats_table.add_row(key.replace('_', ' ').title(), str(value))
        
        console.print(stats_table)
        
    except Exception as e:
        console.print(f"[red]SBD staging processing failed: {e}[/red]")
        logger.error(f"SBD staging error: {e}")
        raise click.ClickException(str(e))


@pipeline_group.command("sbd-test")
@click.option("--limit", type=int, default=5, help="Number of records to test with")
def sbd_test_command(limit: int):
    """SBD test command wrapper that runs async function."""
    asyncio.run(_sbd_test_async(limit))


async def _sbd_test_async(limit: int):
    """
    Test SBD staging processor with a small sample of data.
    
    Examples:
    \b
        # Test with 5 records (default)
        uv run -m src.interfaces.cli pipeline sbd-test
        
        # Test with 10 records
        uv run -m src.interfaces.cli pipeline sbd-test --limit 10
    """
    try:
        console.print(f"[blue]Testing SBD Staging Processor with {limit} records[/blue]")
        
        # Show raw data sample
        from ....data.database.connection import get_connection
        db_connection = get_connection()
        
        async with db_connection.get_async_connection() as connection:
            raw_sample = await connection.fetch(f"""
                SELECT id, external_matchup_id, 
                       raw_response->'game_data'->>'game_name' as game_name,
                       raw_response->'betting_record'->>'sportsbook' as sportsbook,
                       raw_response->'betting_record'->>'bet_type' as bet_type,
                       collected_at
                FROM raw_data.sbd_betting_splits 
                WHERE processed_at IS NULL
                ORDER BY collected_at DESC 
                LIMIT {limit}
            """)
        
        if not raw_sample:
            console.print("[yellow]No unprocessed SBD records found for testing[/yellow]")
            return
        
        # Show sample data
        raw_table = Table(title=f"Raw SBD Data Sample ({len(raw_sample)} records)")
        raw_table.add_column("ID", style="cyan")
        raw_table.add_column("Game", style="green")
        raw_table.add_column("Sportsbook", style="magenta")
        raw_table.add_column("Bet Type", style="yellow")
        raw_table.add_column("Collected", style="blue")
        
        for record in raw_sample:
            raw_table.add_row(
                str(record['id']),
                record['game_name'] or 'N/A',
                record['sportsbook'] or 'N/A',
                record['bet_type'] or 'N/A',
                record['collected_at'].strftime('%H:%M:%S') if record['collected_at'] else 'N/A'
            )
        
        console.print(raw_table)
        
        # Process with SBD staging processor
        console.print(f"\n[blue]Processing {limit} records through SBD staging processor...[/blue]")
        
        from ....data.pipeline.zone_interface import create_zone_config, ZoneType
        settings = get_settings()
        
        config = create_zone_config(ZoneType.STAGING, settings.schemas.staging)
        processor = SBDStagingProcessor(config)
        
        result = await processor.process_sbd_raw_records(limit=limit)
        
        # Show results
        if result.status == ProcessingStatus.COMPLETED:
            console.print(f"[green]✅ Test completed successfully[/green]")
            console.print(f"[blue]Records processed: {result.records_processed}[/blue]")
            console.print(f"[green]Records successful: {result.records_successful}[/green]")
            
            # Show processed staging data
            db_connection = get_connection()
            async with db_connection.get_async_connection() as connection:
                staging_sample = await connection.fetch("""
                    SELECT g.home_team_normalized, g.away_team_normalized, 
                           bs.sportsbook_name, bs.bet_type, 
                           bs.public_bet_percentage, bs.public_money_percentage,
                           bs.sharp_bet_percentage, bs.data_source
                    FROM staging.betting_splits bs
                    JOIN staging.games g ON bs.game_id = g.id
                    ORDER BY bs.created_at DESC
                    LIMIT $1
                """, limit)
                
                if staging_sample:
                    staging_table = Table(title="Processed Staging Data")
                    staging_table.add_column("Game", style="green")
                    staging_table.add_column("Book", style="magenta")
                    staging_table.add_column("Type", style="yellow")
                    staging_table.add_column("Bets %", style="cyan")
                    staging_table.add_column("Money %", style="blue")
                    staging_table.add_column("Sharp %", style="red")
                    staging_table.add_column("Source", style="bright_black")
                    
                    for record in staging_sample:
                        game = f"{record['away_team_normalized']} @ {record['home_team_normalized']}"
                        staging_table.add_row(
                            game,
                            record['sportsbook_name'],
                            record['bet_type'],
                            f"{record['public_bet_percentage']:.1f}" if record['public_bet_percentage'] else 'N/A',
                            f"{record['public_money_percentage']:.1f}" if record['public_money_percentage'] else 'N/A',
                            f"{record['sharp_bet_percentage']:.1f}" if record['sharp_bet_percentage'] else '0.0',
                            record['data_source'] or 'unknown'
                        )
                    
                    console.print(staging_table)
            
        else:
            console.print(f"[red]❌ Test failed[/red]")
            if result.errors:
                for error in result.errors:
                    console.print(f"[red]Error: {error}[/red]")
                    
    except Exception as e:
        console.print(f"[red]SBD test failed: {e}[/red]")
        logger.error(f"SBD test error: {e}")
        raise click.ClickException(str(e))


# Make the command available for import
pipeline = pipeline_group