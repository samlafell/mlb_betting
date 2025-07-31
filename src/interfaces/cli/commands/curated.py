"""
CURATED Zone Management CLI Commands

Commands for managing STAGING → CURATED data processing pipeline.
Provides the critical missing CLI interface for CURATED zone operations.

This resolves the gap in CLI commands for CURATED processing identified
in the STAGING → CURATED gap analysis.

Reference: docs/STAGING_CURATED_GAP_ANALYSIS.md - CLI Integration Requirements
"""

import asyncio
from typing import Any

import click
from rich.console import Console
from rich.panel import Panel
from rich.progress import (
    BarColumn,
    Progress,
    SpinnerColumn,
    TextColumn,
    TimeRemainingColumn,
)
from rich.table import Table

from ....core.config import get_settings
from ....core.logging import LogComponent, get_logger

# Import guards for service dependencies - addresses critical code review issue
SERVICES_AVAILABLE = {
    "enhanced_games": True,
    "ml_temporal_features": True,
    "betting_splits_aggregator": True,
    "staging_curated_orchestrator": True,
}
SERVICE_IMPORT_ERRORS = {}

try:
    from ....services.curated_zone.enhanced_games_service import EnhancedGamesService
except ImportError as e:
    SERVICES_AVAILABLE["enhanced_games"] = False
    SERVICE_IMPORT_ERRORS["enhanced_games"] = str(e)
    EnhancedGamesService = None

try:
    from ....services.curated_zone.ml_temporal_features_service import (
        MLTemporalFeaturesService,
    )
except ImportError as e:
    SERVICES_AVAILABLE["ml_temporal_features"] = False
    SERVICE_IMPORT_ERRORS["ml_temporal_features"] = str(e)
    MLTemporalFeaturesService = None

try:
    from ....services.curated_zone.betting_splits_aggregator import (
        BettingSplitsAggregator,
    )
except ImportError as e:
    SERVICES_AVAILABLE["betting_splits_aggregator"] = False
    SERVICE_IMPORT_ERRORS["betting_splits_aggregator"] = str(e)
    BettingSplitsAggregator = None

try:
    from ....services.curated_zone.staging_curated_orchestrator import (
        ProcessingMode,
        ProcessingStatus,
        StagingCuratedOrchestrator,
    )
except ImportError as e:
    SERVICES_AVAILABLE["staging_curated_orchestrator"] = False
    SERVICE_IMPORT_ERRORS["staging_curated_orchestrator"] = str(e)
    ProcessingMode = None
    ProcessingStatus = None
    StagingCuratedOrchestrator = None

logger = get_logger(__name__, LogComponent.CLI)
console = Console()


def _validate_service_dependencies():
    """Validate that required services are available."""
    unavailable_services = [
        name for name, available in SERVICES_AVAILABLE.items() if not available
    ]

    if unavailable_services:
        console.print(
            f"[red]❌ Required services not available: {', '.join(unavailable_services)}[/red]"
        )
        console.print("\n[yellow]Service import errors:[/yellow]")
        for service in unavailable_services:
            error = SERVICE_IMPORT_ERRORS.get(service, "Unknown error")
            console.print(f"  • {service}: {error}")

        console.print("\n[bold]This may be because:[/bold]")
        console.print("• Services are not yet implemented")
        console.print("• Missing Python dependencies")
        console.print("• Import path issues")

        console.print("\n[bold]Try:[/bold]")
        console.print("• Check service implementations in src/services/curated_zone/")
        console.print(
            "• [bold cyan]uv run -m src.interfaces.cli curated check-deps[/bold cyan]"
        )
        console.print("• [bold cyan]uv sync --dev[/bold cyan] to install dependencies")
        raise click.Abort()


def _get_config():
    """Get centralized configuration settings."""
    try:
        return get_settings()
    except Exception as e:
        console.print(f"[red]❌ Configuration error: {e}[/red]")
        raise click.Abort() from e


def _validate_database_config():
    """Validate database configuration for curated operations."""
    config = _get_config()

    if not config.database.is_configuration_complete():
        issues = config.database.get_connection_issues()
        console.print("[red]❌ Database configuration issues:[/red]")
        for issue in issues:
            console.print(f"  • {issue}")
        console.print("\n[yellow]Fix database configuration and try again.[/yellow]")
        raise click.Abort()

    return config


@click.group(name="curated")
def curated_group():
    """
    CURATED Zone Management Commands

    Manage STAGING → CURATED data processing pipeline.
    Critical commands for resolving the 0% game coverage gap.

    Includes comprehensive dependency validation and error handling.
    """
    pass


@curated_group.command("check-deps")
def check_dependencies():
    """Check service dependencies and system readiness."""
    console.print("[bold blue]Checking CURATED Zone Dependencies[/bold blue]")

    # Check service availability
    console.print("\n[bold]Service Dependencies:[/bold]")
    for service_name, available in SERVICES_AVAILABLE.items():
        status = "✅ Available" if available else "❌ Missing"
        console.print(f"  • {service_name}: {status}")
        if not available:
            error = SERVICE_IMPORT_ERRORS.get(service_name, "Unknown error")
            console.print(f"    [dim]Error: {error}[/dim]")

    # Check configuration
    console.print("\n[bold]Configuration:[/bold]")
    try:
        config = _get_config()
        console.print("  • Configuration loaded: ✅")

        if config.database.is_configuration_complete():
            console.print("  • Database config: ✅ Complete")
        else:
            console.print("  • Database config: ❌ Issues found")
            issues = config.database.get_connection_issues()
            for issue in issues:
                console.print(f"    [dim]- {issue}[/dim]")

    except Exception as e:
        console.print(f"  • Configuration: ❌ Error - {e}")

    # Summary
    all_services_available = all(SERVICES_AVAILABLE.values())
    config_valid = False
    try:
        config = _get_config()
        config_valid = config.database.is_configuration_complete()
    except Exception:
        pass

    if all_services_available and config_valid:
        console.print("\n[green]🎉 All dependencies ready![/green]")
    else:
        console.print("\n[yellow]⚠️  Some dependencies need attention[/yellow]")
        console.print("\n[bold]Next steps:[/bold]")
        if not all_services_available:
            console.print("• Implement missing services in src/services/curated_zone/")
        if not config_valid:
            console.print("• Fix database configuration")
        console.print(
            "• Run [bold cyan]uv sync --dev[/bold cyan] to install dependencies"
        )


@curated_group.command("process-games")
@click.option(
    "--days-back",
    type=click.IntRange(1, 365),  # Input validation - addresses code review issue
    default=7,
    help="Days to look back for staging games (1-365, default: 7)",
)
@click.option(
    "--limit",
    type=click.IntRange(1, 10000),  # Input validation for reasonable limits
    help="Limit number of games to process (1-10000, for testing)",
)
@click.option(
    "--dry-run", is_flag=True, help="Show what would be processed without executing"
)
@click.option(
    "--check-deps", is_flag=True, help="Validate service availability before execution"
)
def process_games(days_back: int, limit: int | None, dry_run: bool, check_deps: bool):
    """Process games command wrapper that runs async function."""
    asyncio.run(_process_games_async(days_back, limit, dry_run, check_deps))


async def _process_games_async(
    days_back: int, limit: int | None, dry_run: bool, check_deps: bool = False
):
    """
    Process staging games into curated enhanced_games.

    This is the critical command to resolve 0% game coverage.

    Examples:
    \b
        # Process recent games (resolves 0% coverage issue)
        uv run -m src.interfaces.cli curated process-games --days-back 7

        # Test with limited games
        uv run -m src.interfaces.cli curated process-games --limit 10

        # Dry run to see what would be processed
        uv run -m src.interfaces.cli curated process-games --dry-run
    """
    try:
        # Validate dependencies if requested or if services are missing
        if check_deps or not all(SERVICES_AVAILABLE.values()):
            _validate_service_dependencies()

        # Validate configuration
        config = _validate_database_config()

        if dry_run:
            console.print(
                "[yellow]DRY RUN MODE - No actual processing will occur[/yellow]"
            )

        console.print("[blue]Processing staging games to curated zone...[/blue]")
        console.print(f"Days back: {days_back}, Limit: {limit or 'None'}")
        console.print(f"Database: {config.database.masked_connection_string}")

        # Create enhanced games service with error handling
        if EnhancedGamesService is None:
            console.print("[red]❌ EnhancedGamesService not available[/red]")
            console.print("[yellow]Using mock service for demonstration[/yellow]")
            # Return mock result instead of failing
            mock_result = type(
                "MockResult",
                (),
                {
                    "games_processed": 0,
                    "games_successful": 0,
                    "games_failed": 0,
                    "processing_time_seconds": 0.0,
                    "errors": [
                        "EnhancedGamesService not implemented - this is a mock response"
                    ],
                },
            )()
            _display_processing_results(mock_result, "Enhanced Games Processing (Mock)")
            return

        service = EnhancedGamesService()

        # Execute processing with progress indicator
        with Progress(
            SpinnerColumn(),
            TextColumn("[progress.description]{task.description}"),
            BarColumn(),
            TimeRemainingColumn(),
            console=console,
        ) as progress:
            task = progress.add_task("Processing games...", total=None)

            result = await service.process_recent_games(
                days_back=days_back, limit=limit, dry_run=dry_run
            )

            progress.update(task, completed=True)

        # Display results
        _display_processing_results(result, "Enhanced Games Processing")

    except Exception as e:
        console.print(f"[red]Enhanced games processing failed: {e}[/red]")
        logger.error(f"Enhanced games processing error: {e}")
        raise click.ClickException(str(e)) from e


@curated_group.command("run-pipeline")
@click.option(
    "--mode",
    type=click.Choice(["full", "games-only"]),  # Removed unimplemented modes
    default="full",
    help="Processing mode (default: full)",
)
@click.option(
    "--days-back",
    type=click.IntRange(1, 365),  # Added input validation as per code review
    default=7,
    help="Days to look back for staging data (1-365 days, default: 7)",
)
@click.option(
    "--limit",
    type=click.IntRange(1, 10000),  # Added input validation for limit
    help="Limit processing for testing (1-10000 games)",
)
@click.option(
    "--dry-run", is_flag=True, help="Show what would be processed without executing"
)
@click.option(
    "--check-deps", is_flag=True, help="Validate service availability before execution"
)
def run_pipeline(
    mode: str, days_back: int, limit: int | None, dry_run: bool, check_deps: bool
):
    """Pipeline command wrapper that runs async function."""
    asyncio.run(_run_pipeline_async(mode, days_back, limit, dry_run, check_deps))


async def _run_pipeline_async(
    mode: str,
    days_back: int,
    limit: int | None,
    dry_run: bool,
    check_deps: bool = False,
):
    """
    Run STAGING → CURATED processing pipeline.

    Orchestrated processing of all CURATED zone components with dependency validation.

    Examples:
    \b
        # Run full pipeline (recommended for resolving gaps)
        uv run -m src.interfaces.cli curated run-pipeline --mode full

        # Process only enhanced games
        uv run -m src.interfaces.cli curated run-pipeline --mode games-only

        # Test pipeline with limited data
        uv run -m src.interfaces.cli curated run-pipeline --limit 10 --dry-run
    """
    try:
        # Validate dependencies if requested or if services are missing
        if check_deps or not all(SERVICES_AVAILABLE.values()):
            _validate_service_dependencies()

        # Validate configuration
        config = _validate_database_config()

        if dry_run:
            console.print(
                "[yellow]DRY RUN MODE - No actual processing will occur[/yellow]"
            )

        console.print(f"[blue]Running STAGING → CURATED pipeline: mode={mode}[/blue]")
        console.print(f"Days back: {days_back}, Limit: {limit or 'None'}")
        console.print(f"Database: {config.database.masked_connection_string}")

        # Create orchestrator with error handling
        if StagingCuratedOrchestrator is None:
            console.print("[red]❌ StagingCuratedOrchestrator not available[/red]")
            console.print("[yellow]Using mock orchestrator for demonstration[/yellow]")
            # Return mock result
            mock_result = type(
                "MockResult",
                (),
                {
                    "total_processed": 0,
                    "successful": 0,
                    "failed": 0,
                    "processing_time_seconds": 0.0,
                    "errors": [
                        "StagingCuratedOrchestrator not implemented - this is a mock response"
                    ],
                },
            )()
            _display_orchestration_results(mock_result)
            return

        orchestrator = StagingCuratedOrchestrator()

        # Execute based on mode
        with Progress(
            SpinnerColumn(),
            TextColumn("[progress.description]{task.description}"),
            BarColumn(),
            TimeRemainingColumn(),
            console=console,
        ) as progress:
            task = progress.add_task(f"Running {mode} pipeline...", total=None)

            if mode == "full":
                result = await orchestrator.run_full_processing(
                    days_back=days_back, limit=limit, dry_run=dry_run
                )
            elif mode == "games-only":
                result = await orchestrator.run_games_only_processing(
                    days_back=days_back, limit=limit, dry_run=dry_run
                )
            else:
                # This should never happen due to Click validation, but defensive programming
                console.print(f"[red]❌ Invalid mode: {mode}[/red]")
                raise click.ClickException(f"Invalid mode: {mode}")

            progress.update(task, completed=True)

        # Display orchestration results
        _display_orchestration_results(result)

    except Exception as e:
        console.print(f"[red]Pipeline execution failed: {e}[/red]")
        logger.error(f"Pipeline execution error: {e}")
        raise click.ClickException(str(e))


@curated_group.command("status")
@click.option("--detailed", is_flag=True, help="Show detailed status information")
@click.option("--coverage", is_flag=True, help="Show coverage analysis")
@click.option(
    "--check-deps",
    is_flag=True,
    help="Validate service availability before status check",
)
def status_command(detailed: bool, coverage: bool, check_deps: bool):
    """Status command wrapper that runs async function."""
    asyncio.run(_status_async(detailed, coverage, check_deps))


async def _status_async(detailed: bool, coverage: bool, check_deps: bool = False):
    """
    Check CURATED zone status and processing gaps.

    Essential command for monitoring the pipeline health and coverage.

    Examples:
    \b
        # Check overall status
        uv run -m src.interfaces.cli curated status

        # Detailed status with coverage analysis
        uv run -m src.interfaces.cli curated status --detailed --coverage

        # Full dependency validation
        uv run -m src.interfaces.cli curated status --check-deps --detailed
    """
    try:
        # Validate dependencies if requested or if services are missing
        if check_deps or not all(SERVICES_AVAILABLE.values()):
            _validate_service_dependencies()

        # Validate configuration
        config = _validate_database_config()

        console.print("[blue]Checking CURATED zone status...[/blue]")
        console.print(f"Database: {config.database.masked_connection_string}")

        # Create orchestrator for comprehensive status with error handling
        if StagingCuratedOrchestrator is None:
            console.print("[red]❌ StagingCuratedOrchestrator not available[/red]")
            console.print("[yellow]Showing basic status information only[/yellow]")

            # Basic status without orchestrator
            console.print("\n[yellow]Service Availability:[/yellow]")
            for service, available in SERVICES_AVAILABLE.items():
                status_icon = "✅" if available else "❌"
                console.print(f"  {status_icon} {service}")
            return

        orchestrator = StagingCuratedOrchestrator()

        # Get health check
        health = await orchestrator.health_check()

        # Display health status
        _display_health_status(health)

        if coverage:
            # Display coverage analysis
            coverage_stats = await orchestrator.get_curated_coverage_stats()
            _display_coverage_analysis(coverage_stats)

        if detailed:
            # Display detailed statistics
            stats = await orchestrator.get_orchestration_stats()
            _display_detailed_stats(stats)

    except Exception as e:
        console.print(f"[red]Status check failed: {e}[/red]")
        logger.error(f"Status check error: {e}")
        raise click.ClickException(str(e))


@curated_group.command("health-check")
@click.option(
    "--check-deps",
    is_flag=True,
    help="Validate service availability during health check",
)
def health_check(check_deps: bool):
    """Health check command wrapper that runs async function."""
    asyncio.run(_health_check_async(check_deps))


async def _health_check_async(check_deps: bool = False):
    """
    Comprehensive health check for STAGING → CURATED pipeline.

    Critical monitoring command for production systems.
    """
    try:
        console.print("[blue]Running CURATED zone health check...[/blue]")

        # Validate dependencies if requested
        if check_deps or not all(SERVICES_AVAILABLE.values()):
            _validate_service_dependencies()

        # Validate configuration
        config = _validate_database_config()
        console.print(f"Database: {config.database.masked_connection_string}")

        # Handle missing orchestrator gracefully
        if StagingCuratedOrchestrator is None:
            console.print("[red]❌ StagingCuratedOrchestrator not available[/red]")
            console.print("[yellow]Showing basic health information only[/yellow]")

            # Basic health check without orchestrator
            console.print("\n[yellow]Service Health Summary:[/yellow]")
            healthy_services = sum(1 for s in SERVICES_AVAILABLE.values() if s)
            total_services = len(SERVICES_AVAILABLE)
            health_percentage = (healthy_services / total_services) * 100

            health_color = (
                "red"
                if health_percentage < 50
                else "yellow"
                if health_percentage < 80
                else "green"
            )
            console.print(
                f"[{health_color}]Service Health: {health_percentage:.1f}% ({healthy_services}/{total_services})[/{health_color}]"
            )
            return

        orchestrator = StagingCuratedOrchestrator()
        health = await orchestrator.health_check()

        # Display health status with appropriate styling
        status = health.get("status", "unknown")
        status_color = {
            "healthy": "green",
            "warning": "yellow",
            "critical": "red",
            "unhealthy": "red",
        }.get(status, "white")

        console.print(
            f"\n[{status_color}]Overall Status: {status.upper()}[/{status_color}]"
        )

        # Show issues if any
        issues = health.get("issues", [])
        if issues:
            console.print("\n[red]Issues Detected:[/red]")
            for issue in issues:
                console.print(f"  • {issue}")

        # Show key metrics
        processing_lag = health.get("processing_lag_hours", 0)
        if processing_lag > 0:
            lag_color = (
                "red"
                if processing_lag > 24
                else "yellow"
                if processing_lag > 6
                else "green"
            )
            console.print(
                f"\n[{lag_color}]Processing Lag: {processing_lag:.1f} hours[/{lag_color}]"
            )

        coverage_stats = health.get("coverage_stats", {})
        coverage_pct = coverage_stats.get("coverage_percentage", 0)
        coverage_color = (
            "red" if coverage_pct < 50 else "yellow" if coverage_pct < 80 else "green"
        )
        console.print(
            f"[{coverage_color}]Game Coverage: {coverage_pct:.1f}%[/{coverage_color}]"
        )

        # Service implementation status
        services = health.get("services_implemented", {})
        console.print("\n[blue]Service Implementation Status:[/blue]")
        for service, implemented in services.items():
            status_icon = "✅" if implemented else "❌"
            console.print(f"  {status_icon} {service.replace('_', ' ').title()}")

        # Exit with appropriate code for monitoring systems
        if status in ["critical", "unhealthy"]:
            raise click.ClickException("Health check failed")

    except Exception as e:
        console.print(f"[red]Health check failed: {e}[/red]")
        raise click.ClickException(str(e))


def _display_processing_results(result: Any, title: str):
    """Display processing results in a formatted table."""

    console.print(f"\n[green]✅ {title} Results[/green]")
    console.print("=" * 60)

    # Overview
    console.print(f"Games Processed: {result.games_processed}")
    console.print(f"Games Successful: {result.games_successful}")
    console.print(f"Games Failed: {result.games_failed}")
    console.print(f"Processing Time: {result.processing_time_seconds:.2f} seconds")

    if result.games_processed > 0:
        success_rate = (result.games_successful / result.games_processed) * 100
        console.print(f"Success Rate: {success_rate:.1f}%")

    # Errors
    if result.errors:
        console.print(f"\n[red]Errors ({len(result.errors)}):[/red]")
        for error in result.errors[:5]:  # Show first 5 errors
            console.print(f"  • {error}")
        if len(result.errors) > 5:
            console.print(f"  ... and {len(result.errors) - 5} more errors")


def _display_orchestration_results(result: Any):
    """Display orchestration results."""

    # Status styling
    status_color = {
        ProcessingStatus.COMPLETED: "green",
        ProcessingStatus.PARTIAL: "yellow",
        ProcessingStatus.FAILED: "red",
        ProcessingStatus.IN_PROGRESS: "blue",
    }.get(result.status, "white")

    console.print(
        f"\n[{status_color}]Pipeline Status: {result.status.value.upper()}[/{status_color}]"
    )
    console.print("=" * 60)

    # Processing results
    console.print(f"Mode: {result.mode.value}")
    console.print(f"Games Processed: {result.games_processed}")
    console.print(f"Features Processed: {result.features_processed}")
    console.print(f"Splits Processed: {result.splits_processed}")
    console.print(f"Processing Time: {result.processing_time_seconds:.2f} seconds")
    console.print(f"Success Rate: {result.success_rate:.1%}")

    # Warnings
    if result.warnings:
        console.print(f"\n[yellow]Warnings ({len(result.warnings)}):[/yellow]")
        for warning in result.warnings:
            console.print(f"  • {warning}")

    # Errors
    if result.errors:
        console.print(f"\n[red]Errors ({len(result.errors)}):[/red]")
        for error in result.errors[:5]:
            console.print(f"  • {error}")
        if len(result.errors) > 5:
            console.print(f"  ... and {len(result.errors) - 5} more errors")


def _display_health_status(health: dict[str, Any]):
    """Display health status information."""

    status = health.get("status", "unknown")
    status_color = {
        "healthy": "green",
        "warning": "yellow",
        "critical": "red",
        "unhealthy": "red",
    }.get(status, "white")

    console.print(
        Panel(
            f"[bold]CURATED Zone Health Status[/bold]\n\n"
            f"Status: [{status_color}]{status.upper()}[/]\n"
            f"Processing Lag: {health.get('processing_lag_hours', 'Unknown'):.1f} hours\n"
            f"Coverage: {health.get('coverage_stats', {}).get('coverage_percentage', 0):.1f}%",
            title="Health Check",
            border_style=status_color,
        )
    )


def _display_coverage_analysis(coverage_stats: dict[str, Any]):
    """Display coverage analysis table."""

    table = Table(title="STAGING → CURATED Coverage Analysis")
    table.add_column("Metric", style="cyan")
    table.add_column("Value", style="magenta")
    table.add_column("Status", style="green")

    # Coverage metrics
    staging_games = coverage_stats.get("staging_games_recent", 0)
    curated_games = coverage_stats.get("curated_games_recent", 0)
    coverage_pct = coverage_stats.get("coverage_percentage", 0)
    missing_games = coverage_stats.get("missing_games", 0)

    # Status indicators
    coverage_status = (
        "🟢 Good"
        if coverage_pct >= 80
        else "🟡 Poor"
        if coverage_pct >= 50
        else "🔴 Critical"
    )

    table.add_row("Staging Games (7d)", str(staging_games), "")
    table.add_row("Curated Games (7d)", str(curated_games), "")
    table.add_row("Coverage Percentage", f"{coverage_pct:.1f}%", coverage_status)
    table.add_row(
        "Missing Games",
        str(missing_games),
        "🔴 Gap" if missing_games > 0 else "✅ None",
    )

    console.print(table)


def _display_detailed_stats(stats: dict[str, Any]):
    """Display detailed orchestration statistics."""

    console.print("\n[blue]Detailed Statistics[/blue]")
    console.print("-" * 40)

    # Orchestration stats
    console.print(f"Total Runs: {stats.get('total_runs', 0)}")
    console.print(f"Successful Runs: {stats.get('successful_runs', 0)}")
    console.print(f"Failed Runs: {stats.get('failed_runs', 0)}")
    console.print(
        f"Average Processing Time: {stats.get('average_processing_time', 0):.2f}s"
    )

    last_run = stats.get("last_run")
    if last_run:
        console.print(f"Last Run: {last_run}")


# ============================================================================
# ML Temporal Features Commands
# ============================================================================


@curated_group.command("process-ml-features")
@click.option("--game-id", type=int, help="Specific game ID to process")
@click.option("--days-back", type=int, default=7, help="Days to look back for games")
@click.option("--limit", type=int, help="Limit number of games to process")
@click.option(
    "--dry-run", is_flag=True, help="Show what would be processed without executing"
)
def process_ml_features(
    game_id: int | None, days_back: int, limit: int | None, dry_run: bool
):
    """
    Process ML temporal features for games.

    Generates ML-ready temporal features with 60-minute cutoff enforcement.
    """

    if dry_run:
        console.print("[yellow]DRY RUN MODE - No actual processing will occur[/yellow]")

    console.print("Processing ML temporal features...")

    if game_id:
        console.print(f"Game ID: {game_id}")
    else:
        console.print(f"Days back: {days_back}, Limit: {limit}")

    # Run the processing
    result = asyncio.run(_process_ml_features_async(game_id, days_back, limit, dry_run))

    # Display results
    _display_ml_features_results(result, dry_run)


@curated_group.command("ml-features-status")
def ml_features_status():
    """
    Check ML temporal features processing status.

    Shows feature generation statistics and health metrics.
    """
    console.print("Checking ML temporal features status...")

    result = asyncio.run(_get_ml_features_status_async())
    _display_ml_features_status(result)


@curated_group.command("ml-features-health")
def ml_features_health():
    """
    Perform ML temporal features health check.

    Validates service health and database connectivity.
    """
    console.print("Running ML temporal features health check...")

    result = asyncio.run(_get_ml_features_health_async())
    _display_ml_features_health(result)


# ============================================================================
# ML Features Async Functions
# ============================================================================


async def _process_ml_features_async(
    game_id: int | None, days_back: int, limit: int | None, dry_run: bool
) -> dict[str, Any]:
    """Process ML features asynchronously."""

    service = MLTemporalFeaturesService()

    if game_id:
        # Process specific game
        result = await service.process_ml_features(game_id, dry_run=dry_run)
        return {
            "type": "single_game",
            "game_id": game_id,
            "result": result,
            "success": len(result.errors) == 0,
        }
    else:
        # Process multiple games (need to implement batch processing)
        # For now, return placeholder
        return {
            "type": "batch",
            "message": "Batch ML features processing not yet implemented",
            "days_back": days_back,
            "limit": limit,
            "success": False,
        }


async def _get_ml_features_status_async() -> dict[str, Any]:
    """Get ML features status asynchronously."""

    service = MLTemporalFeaturesService()
    return await service.get_processing_stats()


async def _get_ml_features_health_async() -> dict[str, Any]:
    """Get ML features health asynchronously."""

    service = MLTemporalFeaturesService()
    return await service.health_check()


# ============================================================================
# Betting Splits Async Functions
# ============================================================================


async def _process_betting_splits_async(
    game_id: int | None, days_back: int, limit: int | None, dry_run: bool
) -> dict[str, Any]:
    """Process betting splits asynchronously."""

    service = BettingSplitsAggregator()

    if game_id:
        # Process specific game
        result = await service.process_betting_splits(game_id, dry_run=dry_run)
        return {
            "type": "single_game",
            "game_id": game_id,
            "result": result,
            "success": len(result.errors) == 0,
        }
    else:
        # Process multiple games (need to implement batch processing)
        # For now, return placeholder
        return {
            "type": "batch",
            "message": "Batch betting splits processing not yet implemented",
            "days_back": days_back,
            "limit": limit,
            "success": False,
        }


async def _get_betting_splits_status_async() -> dict[str, Any]:
    """Get betting splits status asynchronously."""

    service = BettingSplitsAggregator()
    return await service.get_processing_stats()


async def _get_betting_splits_health_async() -> dict[str, Any]:
    """Get betting splits health asynchronously."""

    service = BettingSplitsAggregator()
    return await service.health_check()


# ============================================================================
# Betting Splits Commands
# ============================================================================


@curated_group.command("process-betting-splits")
@click.option("--game-id", type=int, help="Specific game ID to process")
@click.option("--days-back", type=int, default=7, help="Days to look back for games")
@click.option("--limit", type=int, help="Limit number of games to process")
@click.option(
    "--dry-run", is_flag=True, help="Show what would be processed without executing"
)
def process_betting_splits(
    game_id: int | None, days_back: int, limit: int | None, dry_run: bool
):
    """
    Process betting splits aggregation for games.

    Aggregates betting splits from VSIN, SBD, and Action Network with sharp action detection.
    """

    if dry_run:
        console.print("[yellow]DRY RUN MODE - No actual processing will occur[/yellow]")

    console.print("Processing betting splits aggregation...")

    if game_id:
        console.print(f"Game ID: {game_id}")
    else:
        console.print(f"Days back: {days_back}, Limit: {limit}")

    # Run the processing
    result = asyncio.run(
        _process_betting_splits_async(game_id, days_back, limit, dry_run)
    )

    # Display results
    _display_betting_splits_results(result, dry_run)


@curated_group.command("betting-splits-status")
def betting_splits_status():
    """
    Check betting splits processing status.

    Shows aggregation statistics and sharp action detection metrics.
    """
    console.print("Checking betting splits aggregation status...")

    result = asyncio.run(_get_betting_splits_status_async())
    _display_betting_splits_status(result)


@curated_group.command("betting-splits-health")
def betting_splits_health():
    """
    Perform betting splits health check.

    Validates service health and database connectivity.
    """
    console.print("Running betting splits health check...")

    result = asyncio.run(_get_betting_splits_health_async())
    _display_betting_splits_health(result)


# ============================================================================
# ML Features Display Functions
# ============================================================================


def _display_ml_features_results(result: dict[str, Any], dry_run: bool):
    """Display ML features processing results."""

    with Progress(
        SpinnerColumn(),
        TextColumn("[progress.description]{task.description}"),
        BarColumn(),
        transient=True,
    ) as progress:
        progress.add_task("Processing ML features...", total=None)

    if result["type"] == "single_game":
        game_result = result["result"]

        # Success/failure indicator
        status = "✅" if result["success"] else "❌"
        console.print(f"\n{status} ML Features Processing Results")
        console.print("=" * 60)

        console.print(f"Game ID: {result['game_id']}")
        console.print(f"Features Generated: {game_result.features_generated}")
        console.print(
            f"Processing Time: {game_result.processing_time_seconds:.2f} seconds"
        )
        console.print(f"Data Completeness: {game_result.data_completeness_score:.1%}")
        console.print(
            f"Temporal Coverage: {game_result.temporal_coverage_minutes} minutes"
        )
        console.print(
            f"Cutoff Enforced: {'✅' if game_result.cutoff_enforcement else '❌'}"
        )
        console.print(
            f"Min Data Threshold: {'✅' if game_result.min_data_threshold_met else '❌'}"
        )

        # Feature summaries
        if game_result.line_movement_features:
            console.print("\n[blue]Line Movement Features:[/blue]")
            for key, value in game_result.line_movement_features.items():
                console.print(f"  {key}: {value}")

        if game_result.sharp_action_features:
            console.print("\n[blue]Sharp Action Features:[/blue]")
            for key, value in game_result.sharp_action_features.items():
                console.print(f"  {key}: {value}")

        if game_result.consistency_features:
            console.print("\n[blue]Consistency Features:[/blue]")
            for key, value in game_result.consistency_features.items():
                console.print(f"  {key}: {value}")

        # Errors and warnings
        if game_result.errors:
            console.print(f"\n[red]Errors ({len(game_result.errors)}):[/red]")
            for error in game_result.errors:
                console.print(f"  • {error}")

        if game_result.warnings:
            console.print(f"\n[yellow]Warnings ({len(game_result.warnings)}):[/yellow]")
            for warning in game_result.warnings:
                console.print(f"  • {warning}")

    else:
        # Batch processing results
        console.print(f"\n⚠️ {result['message']}")


def _display_ml_features_status(stats: dict[str, Any]):
    """Display ML features status."""

    # Status panel
    total_features = stats.get("ml_features_total", 0)
    recent_features = stats.get("ml_features_recent", 0)

    status_color = "green" if recent_features > 0 else "red"

    console.print(
        Panel(
            f"Total ML Features: {total_features:,}\n"
            f"Recent Features (24h): {recent_features}\n"
            f"Last Processing: {stats.get('last_run_formatted', 'Never')}",
            title="ML Temporal Features Status",
            border_style=status_color,
        )
    )

    # Processing statistics
    if stats.get("total_games_processed", 0) > 0:
        console.print("\n[blue]Processing Statistics[/blue]")
        console.print("-" * 40)
        console.print(f"Games Processed: {stats.get('total_games_processed', 0)}")
        console.print(f"Features Generated: {stats.get('total_features_generated', 0)}")
        console.print(
            f"Average Processing Time: {stats.get('average_processing_time', 0):.2f}s"
        )


def _display_ml_features_health(health: dict[str, Any]):
    """Display ML features health check results."""

    status = health.get("status", "unknown")
    status_color = "green" if status == "healthy" else "red"

    # Health panel
    console.print(
        Panel(
            f"Status: {status.upper()}\n"
            f"Database: {health.get('database_connection', 'unknown')}\n"
            f"ML Cutoff: {health.get('ml_cutoff_minutes', 60)} minutes\n"
            f"Last Processing: {health.get('last_processing', 'Never')}",
            title="ML Features Health Check",
            border_style=status_color,
        )
    )

    # Processing lag warning
    hours_since_last = health.get("hours_since_last_processing")
    if hours_since_last and hours_since_last > 24:
        console.print(
            f"\n[yellow]⚠️  Warning: No ML features processed in {hours_since_last:.1f} hours[/yellow]"
        )

    # Error details
    if health.get("error"):
        console.print(f"\n[red]Error: {health['error']}[/red]")


# ============================================================================
# Betting Splits Display Functions
# ============================================================================


def _display_betting_splits_results(result: dict[str, Any], dry_run: bool):
    """Display betting splits processing results."""

    with Progress(
        SpinnerColumn(),
        TextColumn("[progress.description]{task.description}"),
        BarColumn(),
        transient=True,
    ) as progress:
        progress.add_task("Processing betting splits...", total=None)

    if result["type"] == "single_game":
        splits_result = result["result"]

        # Success/failure indicator
        status = "✅" if result["success"] else "❌"
        console.print(f"\n{status} Betting Splits Processing Results")
        console.print("=" * 60)

        console.print(f"Game ID: {result['game_id']}")
        console.print(f"Splits Processed: {splits_result.splits_processed}")
        console.print(
            f"Processing Time: {splits_result.processing_time_seconds:.2f} seconds"
        )
        console.print(f"Data Completeness: {splits_result.data_completeness_score:.1%}")
        console.print(f"Source Coverage: {splits_result.source_coverage_score:.1%}")
        console.print(
            f"Sharp Action Detected: {'✅' if splits_result.sharp_action_detected else '❌'}"
        )
        console.print(
            f"Cutoff Enforced: {'✅' if splits_result.cutoff_enforcement else '❌'}"
        )

        # Source breakdown
        console.print("\n[blue]Source Breakdown:[/blue]")
        console.print(f"  VSIN Splits: {splits_result.vsin_splits}")
        console.print(f"  SBD Splits: {splits_result.sbd_splits}")
        console.print(f"  Action Network Splits: {splits_result.action_network_splits}")

        # Sharp action summary
        if splits_result.sharp_action_signals:
            console.print("\n[blue]Sharp Action Signals:[/blue]")
            for key, value in splits_result.sharp_action_signals.items():
                console.print(f"  {key}: {value}")

        # Cross-source conflicts
        if splits_result.cross_source_conflicts > 0:
            console.print(
                f"\n[yellow]Cross-Source Conflicts: {splits_result.cross_source_conflicts}[/yellow]"
            )

        # Errors and warnings
        if splits_result.errors:
            console.print(f"\n[red]Errors ({len(splits_result.errors)}):[/red]")
            for error in splits_result.errors:
                console.print(f"  • {error}")

        if splits_result.warnings:
            console.print(
                f"\n[yellow]Warnings ({len(splits_result.warnings)}):[/yellow]"
            )
            for warning in splits_result.warnings:
                console.print(f"  • {warning}")

    else:
        # Batch processing results
        console.print(f"\n⚠️ {result['message']}")


def _display_betting_splits_status(stats: dict[str, Any]):
    """Display betting splits status."""

    # Status panel
    total_splits = stats.get("betting_splits_total", 0)
    recent_splits = stats.get("betting_splits_recent", 0)

    status_color = "green" if recent_splits > 0 else "red"

    console.print(
        Panel(
            f"Total Betting Splits: {total_splits:,}\n"
            f"Recent Splits (24h): {recent_splits}\n"
            f"Last Processing: {stats.get('last_run_formatted', 'Never')}",
            title="Betting Splits Status",
            border_style=status_color,
        )
    )

    # Processing statistics
    if stats.get("total_games_processed", 0) > 0:
        console.print("\n[blue]Processing Statistics[/blue]")
        console.print("-" * 40)
        console.print(f"Games Processed: {stats.get('total_games_processed', 0)}")
        console.print(f"Splits Generated: {stats.get('total_splits_generated', 0)}")
        console.print(
            f"Average Processing Time: {stats.get('average_processing_time', 0):.2f}s"
        )


def _display_betting_splits_health(health: dict[str, Any]):
    """Display betting splits health check results."""

    status = health.get("status", "unknown")
    status_color = "green" if status == "healthy" else "red"

    # Health panel
    console.print(
        Panel(
            f"Status: {status.upper()}\n"
            f"Database: {health.get('database_connection', 'unknown')}\n"
            f"ML Cutoff: {health.get('ml_cutoff_minutes', 60)} minutes\n"
            f"Last Processing: {health.get('last_processing', 'Never')}",
            title="Betting Splits Health Check",
            border_style=status_color,
        )
    )

    # Processing lag warning
    hours_since_last = health.get("hours_since_last_processing")
    if hours_since_last and hours_since_last > 24:
        console.print(
            f"\n[yellow]⚠️  Warning: No betting splits processed in {hours_since_last:.1f} hours[/yellow]"
        )

    # Error details
    if health.get("error"):
        console.print(f"\n[red]Error: {health['error']}[/red]")


# Make the command available for import
curated = curated_group
