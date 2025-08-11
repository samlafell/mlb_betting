#!/usr/bin/env python3
"""
Batch Collection Commands - Historical Line Movement Collection

Provides CLI commands for collecting historical betting line movement data
over date ranges using the BatchCollectionService infrastructure.
"""

import asyncio
import json
from datetime import datetime, timedelta
from pathlib import Path

import click
from rich.console import Console
from rich.panel import Panel
from rich.progress import (
    BarColumn,
    MofNCompleteColumn,
    Progress,
    SpinnerColumn,
    TextColumn,
)
from rich.table import Table

from ....core.exceptions import DataError
from ....services.batch_collection_service import (
    BatchCollectionConfig,
    BatchCollectionResult,
    BatchCollectionService,
    CollectionStatus,
)

console = Console()


def parse_date(date_str: str) -> datetime:
    """Parse date string to datetime object."""
    try:
        return datetime.strptime(date_str, "%Y-%m-%d")
    except ValueError:
        try:
            return datetime.strptime(date_str, "%Y-%m-%d %H:%M:%S")
        except ValueError:
            raise click.BadParameter(
                f"Invalid date format: {date_str}. Use YYYY-MM-DD or YYYY-MM-DD HH:MM:SS"
            )


def format_duration(duration: timedelta) -> str:
    """Format duration for display."""
    if duration is None:
        return "N/A"

    total_seconds = int(duration.total_seconds())
    hours = total_seconds // 3600
    minutes = (total_seconds % 3600) // 60
    seconds = total_seconds % 60

    if hours > 0:
        return f"{hours}h {minutes}m {seconds}s"
    elif minutes > 0:
        return f"{minutes}m {seconds}s"
    else:
        return f"{seconds}s"


def display_batch_summary(result: BatchCollectionResult):
    """Display comprehensive batch collection summary."""
    summary = result.get_summary()

    # Create summary table
    table = Table(title="Batch Collection Summary")
    table.add_column("Metric", style="cyan")
    table.add_column("Value", style="green")

    table.add_row("Batch ID", summary["batch_id"])
    table.add_row("Date Range", summary["date_range"])
    table.add_row("Duration", summary["duration"] or "N/A")
    table.add_row("Total Games", str(summary["total_games"]))
    table.add_row("Games Processed", str(summary["games_processed"]))
    table.add_row("Games Successful", str(summary["games_successful"]))
    table.add_row("Games Failed", str(summary["games_failed"]))
    table.add_row("Games Skipped", str(summary["games_skipped"]))
    table.add_row("Success Rate", summary["success_rate"])
    table.add_row("Total Records", str(summary["total_records"]))
    table.add_row("Errors", str(summary["errors"]))

    console.print(table)

    # Show failed games if any
    if result.games_failed > 0:
        console.print("\n[yellow]Failed Games:[/yellow]")
        failed_table = Table()
        failed_table.add_column("Game", style="red")
        failed_table.add_column("Error", style="yellow")

        for task in result.tasks:
            if task.status == CollectionStatus.FAILED:
                game_info = f"{task.mlb_game.away_team} @ {task.mlb_game.home_team}"
                error_msg = "; ".join(task.errors[-3:])  # Show last 3 errors
                failed_table.add_row(game_info, error_msg)

        console.print(failed_table)


@click.group()
def batch_collection():
    """
    Historical betting line movement collection commands.

    Provides comprehensive batch collection capabilities for historical
    line movement data over date ranges.
    """
    pass


@batch_collection.command()
@click.option(
    "--start-date",
    type=str,
    required=True,
    help="Start date for collection (YYYY-MM-DD)",
)
@click.option(
    "--end-date", type=str, required=True, help="End date for collection (YYYY-MM-DD)"
)
@click.option(
    "--source",
    type=click.Choice(["action_network", "sports_book_review", "sbr", "vsin", "sbd", "mlb_stats_api"], case_sensitive=False),
    default="action_network",
    help="Data source for collection",
)
@click.option(
    "--max-concurrent", type=int, default=5, help="Maximum concurrent collections"
)
@click.option(
    "--retry-attempts",
    type=int,
    default=3,
    help="Number of retry attempts for failed collections",
)
@click.option(
    "--progress-interval", type=int, default=10, help="Progress update interval (games)"
)
@click.option(
    "--checkpoint-interval",
    type=int,
    default=50,
    help="Checkpoint save interval (games)",
)
@click.option(
    "--output-dir",
    type=click.Path(path_type=Path),
    default="output/batch_collections",
    help="Output directory for results",
)
@click.option(
    "--dry-run", is_flag=True, help="Simulate collection without actual data retrieval"
)
@click.option(
    "--use-enhanced-collector/--no-enhanced-collector",
    default=True,
    help="Use enhanced SBR collector with improved date-based discovery",
)
@click.option("--resume", type=str, help="Resume collection from checkpoint file")
def collect_range(
    start_date: str,
    end_date: str,
    source: str,
    max_concurrent: int,
    retry_attempts: int,
    progress_interval: int,
    checkpoint_interval: int,
    output_dir: Path,
    dry_run: bool,
    resume: str | None,
    use_enhanced_collector: bool,
):
    """
    Collect historical betting lines for a date range.

    Examples:
        # Collect data for March 2025
        uv run -m src.interfaces.cli batch-collection collect-range --start-date 2025-03-15 --end-date 2025-03-20

        # Full season collection with custom settings
        uv run -m src.interfaces.cli batch-collection collect-range --start-date 2025-03-15 --end-date 2025-07-15 --max-concurrent 3 --retry-attempts 2

        # Dry run to test configuration
        uv run -m src.interfaces.cli batch-collection collect-range --start-date 2025-03-15 --end-date 2025-03-20 --dry-run
    """

    async def _collect_range():
        try:
            # Parse dates
            start_dt = parse_date(start_date)
            end_dt = parse_date(end_date)

            # Validate date range
            if start_dt > end_dt:
                raise click.BadParameter("Start date must be before end date")

            # Create output directory
            output_dir.mkdir(parents=True, exist_ok=True)

            # Configure batch collection
            config = BatchCollectionConfig(
                max_concurrent_collections=max_concurrent,
                retry_attempts=retry_attempts,
                progress_update_interval=progress_interval,
                checkpoint_interval=checkpoint_interval,
                checkpoint_directory=str(output_dir / "checkpoints"),
                enable_progress_tracking=True,
                enable_checkpointing=True,
                fail_fast=False,
                skip_failed_games=True,
                use_enhanced_collector=use_enhanced_collector,
            )

            # Create batch collection service
            service = BatchCollectionService(config)

            console.print(
                Panel.fit(
                    f"[bold green]Historical Line Movement Collection[/bold green]\n"
                    f"Date Range: {start_date} to {end_date}\n"
                    f"Source: {source}\n"
                    f"Collector: {'Enhanced' if use_enhanced_collector else 'Legacy'}\n"
                    f"Max Concurrent: {max_concurrent}\n"
                    f"Retry Attempts: {retry_attempts}\n"
                    f"Dry Run: {dry_run}",
                    title="Batch Collection Configuration",
                )
            )

            if dry_run:
                console.print(
                    "[yellow]DRY RUN MODE - No actual data collection will occur[/yellow]"
                )
                return

            # Start collection with progress tracking
            with Progress(
                SpinnerColumn(),
                TextColumn("[progress.description]{task.description}"),
                BarColumn(),
                MofNCompleteColumn(),
                TextColumn("• {task.fields[status]}"),
                console=console,
                transient=False,
            ) as progress:
                # Create progress task
                task = progress.add_task(
                    "Collecting historical data...", total=None, status="Starting"
                )

                # Monitor progress in background
                async def monitor_progress():
                    while True:
                        status = service.get_current_status()
                        if status:
                            if status["total_games"] > 0:
                                progress.update(
                                    task,
                                    total=status["total_games"],
                                    completed=status["games_processed"],
                                    status=f"Success: {status['games_successful']}, Failed: {status['games_failed']}",
                                )
                            else:
                                progress.update(task, status="Discovering games...")

                        await asyncio.sleep(2)

                        if not service.is_running:
                            break

                # Start monitoring
                monitor_task = asyncio.create_task(monitor_progress())

                try:
                    # Execute collection
                    result = await service.collect_date_range(
                        start_date=start_dt, end_date=end_dt, source=source
                    )

                    # Cancel monitoring
                    monitor_task.cancel()

                    # Display results
                    console.print("\n[bold green]Collection Complete![/bold green]")
                    display_batch_summary(result)

                    # Save detailed results
                    results_file = output_dir / f"batch_result_{result.batch_id}.json"
                    with open(results_file, "w") as f:
                        json.dump(result.get_summary(), f, indent=2)

                    console.print(f"\n[cyan]Results saved to: {results_file}[/cyan]")

                    # Show retry suggestion for failed games
                    if result.games_failed > 0:
                        console.print("\n[yellow]To retry failed games, run:[/yellow]")
                        console.print(
                            f"uv run -m src.interfaces.cli batch-collection retry-failed --batch-id {result.batch_id}"
                        )

                except Exception as e:
                    monitor_task.cancel()
                    raise DataError(f"Collection failed: {str(e)}")

        except Exception as e:
            console.print(f"[red]Error: {str(e)}[/red]")
            raise click.Abort()

    # Run async collection
    asyncio.run(_collect_range())


@batch_collection.command()
@click.option(
    "--batch-id", type=str, required=True, help="Batch ID to retry failed games for"
)
@click.option(
    "--output-dir",
    type=click.Path(path_type=Path),
    default="output/batch_collections",
    help="Output directory for checkpoints",
)
def retry_failed(batch_id: str, output_dir: Path):
    """
    Retry failed games from a previous batch collection.

    Examples:
        uv run -m src.interfaces.cli batch-collection retry-failed --batch-id abc123-def456
    """

    async def _retry_failed():
        try:
            # Load checkpoint
            checkpoint_file = output_dir / "checkpoints" / f"batch_{batch_id}.json"

            if not checkpoint_file.exists():
                console.print(
                    f"[red]Checkpoint file not found: {checkpoint_file}[/red]"
                )
                raise click.Abort()

            console.print(f"[cyan]Loading checkpoint: {checkpoint_file}[/cyan]")

            # Create service with default config
            config = BatchCollectionConfig(
                checkpoint_directory=str(output_dir / "checkpoints")
            )
            service = BatchCollectionService(config)

            # This would require enhancing the service to load from checkpoint
            # For now, show the concept
            console.print(
                "[yellow]Retry functionality requires checkpoint loading implementation[/yellow]"
            )
            console.print(
                f"[cyan]Would retry failed games from batch: {batch_id}[/cyan]"
            )

        except Exception as e:
            console.print(f"[red]Error: {str(e)}[/red]")
            raise click.Abort()

    asyncio.run(_retry_failed())


@batch_collection.command()
@click.option(
    "--output-dir",
    type=click.Path(path_type=Path),
    default="output/batch_collections",
    help="Output directory to check for results",
)
def list_batches(output_dir: Path):
    """
    List all batch collection results.

    Examples:
        uv run -m src.interfaces.cli batch-collection list-batches
    """
    try:
        if not output_dir.exists():
            console.print(f"[yellow]No results directory found: {output_dir}[/yellow]")
            return

        # Find all batch result files
        result_files = list(output_dir.glob("batch_result_*.json"))

        if not result_files:
            console.print("[yellow]No batch results found[/yellow]")
            return

        # Display results table
        table = Table(title="Batch Collection Results")
        table.add_column("Batch ID", style="cyan")
        table.add_column("Date Range", style="green")
        table.add_column("Duration", style="yellow")
        table.add_column("Games", style="blue")
        table.add_column("Success Rate", style="green")
        table.add_column("Records", style="magenta")

        for result_file in sorted(result_files):
            try:
                with open(result_file) as f:
                    result = json.load(f)

                table.add_row(
                    result.get("batch_id", "Unknown")[:8] + "...",
                    result.get("date_range", "Unknown"),
                    result.get("duration", "N/A"),
                    f"{result.get('games_successful', 0)}/{result.get('total_games', 0)}",
                    result.get("success_rate", "N/A"),
                    str(result.get("total_records", 0)),
                )

            except Exception as e:
                console.print(f"[red]Error reading {result_file}: {str(e)}[/red]")

        console.print(table)

    except Exception as e:
        console.print(f"[red]Error: {str(e)}[/red]")
        raise click.Abort()


@batch_collection.command()
@click.option(
    "--start-date",
    type=str,
    default="2025-03-15",
    help="Start date for full season collection",
)
@click.option(
    "--end-date",
    type=str,
    default="2025-07-15",
    help="End date for full season collection",
)
@click.option(
    "--source",
    type=click.Choice(["action_network", "sports_book_review", "sbr", "vsin", "sbd", "mlb_stats_api"], case_sensitive=False),
    default="action_network",
    help="Data source for collection",
)
@click.option(
    "--max-concurrent",
    type=int,
    default=3,
    help="Maximum concurrent collections for season-long collection",
)
@click.option(
    "--output-dir",
    type=click.Path(path_type=Path),
    default="output/season_collections",
    help="Output directory for season results",
)
@click.option(
    "--dry-run",
    is_flag=True,
    help="Simulate season collection without actual data retrieval",
)
def collect_season(
    start_date: str,
    end_date: str,
    source: str,
    max_concurrent: int,
    output_dir: Path,
    dry_run: bool,
):
    """
    Collect full season historical betting lines.

    This is a convenience command for collecting an entire MLB season
    with optimized settings for large-scale data collection.

    Examples:
        # Collect full 2025 season from March to July
        uv run -m src.interfaces.cli batch-collection collect-season

        # Custom season dates
        uv run -m src.interfaces.cli batch-collection collect-season --start-date 2025-04-01 --end-date 2025-09-30

        # Dry run to estimate scope
        uv run -m src.interfaces.cli batch-collection collect-season --dry-run
    """
    console.print(
        Panel.fit(
            f"[bold green]Full Season Collection[/bold green]\n"
            f"Date Range: {start_date} to {end_date}\n"
            f"Source: {source}\n"
            f"Max Concurrent: {max_concurrent}\n"
            f"Dry Run: {dry_run}",
            title="Season Collection Configuration",
        )
    )

    if dry_run:
        console.print(
            "[yellow]DRY RUN MODE - This would collect the full season[/yellow]"
        )
        console.print(f"[cyan]Estimated date range: {start_date} to {end_date}[/cyan]")
        console.print(
            "[cyan]Estimated games: ~2,430 games (162 games × 15 teams)[/cyan]"
        )
        console.print(
            "[cyan]Estimated duration: 8-12 hours depending on success rate[/cyan]"
        )
        return

    # Use collect_range with season-optimized settings
    ctx = click.get_current_context()
    ctx.invoke(
        collect_range,
        start_date=start_date,
        end_date=end_date,
        source=source,
        max_concurrent=max_concurrent,
        retry_attempts=2,  # Reduced for season collection
        progress_interval=25,  # More frequent updates for long collection
        checkpoint_interval=100,  # More frequent checkpoints
        output_dir=output_dir,
        dry_run=dry_run,
    )


if __name__ == "__main__":
    batch_collection()
