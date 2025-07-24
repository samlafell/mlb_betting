#!/usr/bin/env python3
"""
Game Outcomes CLI Commands

Commands for checking and updating game outcomes from MLB-StatsAPI.
Integrates with the core_betting.game_outcomes table.

General Balls
"""

import asyncio
import json
from datetime import datetime, timedelta

import click
from rich.console import Console
from rich.panel import Panel
from rich.progress import Progress, SpinnerColumn, TextColumn
from rich.table import Table

from src.data.database.connection import get_connection
from src.services.game_outcome_service import check_game_outcomes, game_outcome_service

console = Console()


@click.group()
def outcomes():
    """Game outcome checking and management commands."""
    pass


@outcomes.command()
@click.option("--start-date", "-s", type=str, help="Start date (YYYY-MM-DD)")
@click.option("--end-date", "-e", type=str, help="End date (YYYY-MM-DD)")
@click.option(
    "--days", "-d", type=int, default=7, help="Number of days to check (default: 7)"
)
@click.option("--force", "-f", is_flag=True, help="Force update even if outcome exists")
@click.option("--verbose", "-v", is_flag=True, help="Show detailed output")
def check(
    start_date: str | None,
    end_date: str | None,
    days: int,
    force: bool,
    verbose: bool,
):
    """Check for completed games and update outcomes."""

    # Determine date range
    if start_date and end_date:
        date_range = (start_date, end_date)
    elif start_date:
        # Start date provided, calculate end date
        start = datetime.strptime(start_date, "%Y-%m-%d")
        end = start + timedelta(days=days)
        date_range = (start_date, end.strftime("%Y-%m-%d"))
    else:
        # Default to last N days
        end = datetime.now()
        start = end - timedelta(days=days)
        date_range = (start.strftime("%Y-%m-%d"), end.strftime("%Y-%m-%d"))

    console.print("\n🏁 [bold blue]Checking Game Outcomes[/bold blue]")
    console.print(f"📅 Date Range: {date_range[0]} to {date_range[1]}")
    console.print(f"🔄 Force Update: {'Yes' if force else 'No'}")

    async def run_check():
        with Progress(
            SpinnerColumn(),
            TextColumn("[progress.description]{task.description}"),
            console=console,
            transient=True,
        ) as progress:
            task = progress.add_task("Checking game outcomes...", total=None)

            try:
                results = await check_game_outcomes(
                    date_range=date_range, force_update=force
                )
                progress.remove_task(task)

                # Display results
                _display_results(results, verbose)

            except Exception as e:
                progress.remove_task(task)
                console.print(f"\n❌ [bold red]Error:[/bold red] {str(e)}")
                return False

        return True

    success = asyncio.run(run_check())
    if success:
        console.print("\n✅ [bold green]Game outcome check completed![/bold green]")
    else:
        console.print("\n❌ [bold red]Game outcome check failed![/bold red]")


@outcomes.command()
@click.option(
    "--days", "-d", type=int, default=7, help="Number of days to show (default: 7)"
)
@click.option(
    "--format",
    "-f",
    type=click.Choice(["table", "json"]),
    default="table",
    help="Output format",
)
def recent(days: int, format: str):
    """Show recent game outcomes."""

    console.print(
        f"\n📊 [bold blue]Recent Game Outcomes[/bold blue] (last {days} days)"
    )

    async def get_recent():
        try:
            outcomes = await game_outcome_service.get_recent_outcomes(days=days)

            if format == "json":
                console.print(json.dumps(outcomes, indent=2, default=str))
            else:
                _display_outcomes_table(outcomes)

        except Exception as e:
            console.print(f"\n❌ [bold red]Error:[/bold red] {str(e)}")

    asyncio.run(get_recent())


@outcomes.command()
@click.option(
    "--game-id", "-g", type=int, required=True, help="Game ID from core_betting.games"
)
@click.option("--force", "-f", is_flag=True, help="Force update even if outcome exists")
def single(game_id: int, force: bool):
    """Check outcome for a single game."""

    console.print("\n🎯 [bold blue]Checking Single Game Outcome[/bold blue]")
    console.print(f"🆔 Game ID: {game_id}")

    async def check_single():
        try:
            # Get game info first
            async with get_connection() as conn:
                async with conn.cursor() as cursor:
                    await cursor.execute(
                        """
                        SELECT id, mlb_stats_api_game_id, home_team, away_team, 
                               game_datetime, game_status
                        FROM core_betting.games 
                        WHERE id = %s
                    """,
                        [game_id],
                    )

                    row = await cursor.fetchone()
                    if not row:
                        console.print(
                            f"❌ [bold red]Game not found:[/bold red] ID {game_id}"
                        )
                        return

                    game_info = {
                        "id": row[0],
                        "mlb_stats_api_game_id": row[1],
                        "home_team": row[2],
                        "away_team": row[3],
                        "game_datetime": row[4],
                        "game_status": row[5],
                    }

            console.print(f"🏟️  {game_info['away_team']} @ {game_info['home_team']}")
            console.print(f"📅 {game_info['game_datetime']}")

            # Check if outcome already exists
            if not force:
                async with get_connection() as conn:
                    async with conn.cursor() as cursor:
                        await cursor.execute(
                            """
                            SELECT home_score, away_score, home_win, over
                            FROM core_betting.game_outcomes 
                            WHERE game_id = %s
                        """,
                            [game_id],
                        )

                        existing = await cursor.fetchone()
                        if existing:
                            console.print(
                                f"ℹ️  [yellow]Outcome already exists:[/yellow] {existing[1]}-{existing[0]} (Use --force to update)"
                            )
                            return

            # Check outcome
            with Progress(
                SpinnerColumn(),
                TextColumn("[progress.description]{task.description}"),
                console=console,
                transient=True,
            ) as progress:
                task = progress.add_task("Checking game outcome...", total=None)

                from ...services.game_outcome_service import MLBStatsAPIClient

                async with MLBStatsAPIClient() as mlb_client:
                    outcome = await game_outcome_service._check_game_outcome(
                        mlb_client, game_info
                    )

                    progress.remove_task(task)

                    if outcome:
                        await game_outcome_service._update_game_outcome(outcome)
                        console.print(
                            "\n✅ [bold green]Game outcome updated![/bold green]"
                        )
                        console.print(
                            f"📊 Final Score: {outcome.away_team} {outcome.away_score} - {outcome.home_score} {outcome.home_team}"
                        )
                        console.print(
                            f"🏆 Winner: {outcome.home_team if outcome.home_win else outcome.away_team}"
                        )

                        if outcome.over is not None:
                            console.print(
                                f"📈 Total: {outcome.over} (line: {outcome.total_line})"
                            )
                        if outcome.home_cover_spread is not None:
                            console.print(
                                f"📊 Spread: {outcome.home_team} {'covered' if outcome.home_cover_spread else 'did not cover'} (line: {outcome.home_spread_line})"
                            )
                    else:
                        console.print("\n⏳ [yellow]Game not completed yet[/yellow]")

        except Exception as e:
            console.print(f"\n❌ [bold red]Error:[/bold red] {str(e)}")

    asyncio.run(check_single())


@outcomes.command()
@click.option(
    "--source",
    "-s",
    type=click.Choice(["action_network", "sbr", "all"]),
    default="all",
    help="Check outcome coverage for specific source",
)
@click.option(
    "--days", "-d", type=int, default=30, help="Number of days to check (default: 30)"
)
@click.option("--show-missing", "-m", is_flag=True, help="Show games missing outcomes")
def coverage(source: str, days: int, show_missing: bool):
    """Check outcome coverage across data sources."""

    console.print(
        f"\n📊 [bold blue]Game Outcome Coverage Analysis[/bold blue] (last {days} days)"
    )
    console.print(f"🎯 Source: {source.upper()}")

    async def check_coverage():
        try:
            # Calculate date range
            from datetime import datetime, timedelta

            end_date = datetime.now()
            start_date = end_date - timedelta(days=days)

            date_range_str = (
                f"{start_date.strftime('%Y-%m-%d')} to {end_date.strftime('%Y-%m-%d')}"
            )

            # Query for coverage data
            if source == "action_network":
                coverage_data = await _get_action_network_coverage(start_date, end_date)
            elif source == "sbr":
                coverage_data = await _get_sbr_coverage(start_date, end_date)
            else:  # all
                coverage_data = await _get_all_sources_coverage(start_date, end_date)

            # Display coverage results
            _display_coverage_table(coverage_data, date_range_str)

            # Show missing games if requested
            if show_missing and coverage_data.get("missing_games"):
                _display_missing_games(coverage_data["missing_games"])

        except Exception as e:
            console.print(f"\n❌ [bold red]Error:[/bold red] {str(e)}")

    asyncio.run(check_coverage())


@outcomes.command()
@click.option(
    "--games",
    "-g",
    type=click.Choice(["recent", "missing", "all"]),
    default="recent",
    help="Which games to verify",
)
@click.option(
    "--days", "-d", type=int, default=7, help="Number of days to verify (default: 7)"
)
@click.option(
    "--fix-missing",
    "-f",
    is_flag=True,
    help="Attempt to fix missing MLB Stats API IDs",
)
def verify(games: str, days: int, fix_missing: bool):
    """Verify outcome data completeness and cross-source consistency."""

    console.print("\n🔍 [bold blue]Game Outcome Verification[/bold blue]")
    console.print(f"🎯 Scope: {games.upper()} games (last {days} days)")

    async def run_verification():
        try:
            # Calculate date range
            from datetime import datetime, timedelta

            end_date = datetime.now()
            start_date = end_date - timedelta(days=days)

            verification_results = await _run_outcome_verification(
                games, start_date, end_date, fix_missing
            )

            _display_verification_results(verification_results)

        except Exception as e:
            console.print(f"\n❌ [bold red]Error:[/bold red] {str(e)}")

    asyncio.run(run_verification())


@outcomes.command()
@click.option(
    "--days", "-d", type=int, default=30, help="Number of days to check (default: 30)"
)
@click.option(
    "--source",
    "-s",
    type=click.Choice(["action_network", "sbr", "vsin", "all"]),
    default="all",
    help="Source to resolve IDs for",
)
@click.option(
    "--dry-run",
    is_flag=True,
    help="Show what would be resolved without making changes",
)
def resolve_ids(days: int, source: str, dry_run: bool):
    """Resolve missing MLB Stats API game IDs to enable outcome checking."""

    console.print(
        f"\n🔧 [bold blue]MLB Game ID Resolution[/bold blue] (last {days} days)"
    )
    console.print(f"🎯 Source: {source.upper()}")
    console.print(f"🧪 Mode: {'DRY RUN' if dry_run else 'LIVE UPDATE'}")

    async def run_resolution():
        try:
            with Progress(
                SpinnerColumn(),
                TextColumn("[progress.description]{task.description}"),
                console=console,
                transient=True,
            ) as progress:
                # Get stats first
                stats_task = progress.add_task(
                    "Getting resolution statistics...", total=None
                )

                from src.services.game_id_resolution_service import (
                    game_id_resolution_service,
                )

                await game_id_resolution_service.initialize()

                try:
                    stats = await game_id_resolution_service.get_resolution_stats(
                        days=days
                    )
                    progress.remove_task(stats_task)

                    # Display current stats
                    _display_resolution_stats(stats, days)

                    if stats.get("resolvable_missing", 0) == 0:
                        console.print(
                            "\n✅ [green]No resolvable missing game IDs found![/green]"
                        )
                        return

                    # Run resolution
                    resolution_task = progress.add_task(
                        "Resolving missing game IDs...", total=None
                    )

                    source_filter = None if source == "all" else source
                    results = await game_id_resolution_service.resolve_missing_game_ids(
                        days=days, source_filter=source_filter, dry_run=dry_run
                    )

                    progress.remove_task(resolution_task)

                    # Display results
                    _display_resolution_results(results, dry_run)

                finally:
                    await game_id_resolution_service.cleanup()

        except Exception as e:
            console.print(f"\n❌ [bold red]Error:[/bold red] {str(e)}")

    asyncio.run(run_resolution())


@outcomes.command()
@click.option(
    "--days", "-d", type=int, default=30, help="Number of days to analyze (default: 30)"
)
def resolution_stats(days: int):
    """Show MLB Stats API game ID resolution statistics."""

    console.print(
        f"\n📊 [bold blue]Game ID Resolution Statistics[/bold blue] (last {days} days)"
    )

    async def get_stats():
        try:
            from src.services.game_id_resolution_service import (
                game_id_resolution_service,
            )

            await game_id_resolution_service.initialize()

            try:
                stats = await game_id_resolution_service.get_resolution_stats(days=days)
                _display_resolution_stats(stats, days)

            finally:
                await game_id_resolution_service.cleanup()

        except Exception as e:
            console.print(f"\n❌ [bold red]Error:[/bold red] {str(e)}")

    asyncio.run(get_stats())


@outcomes.command()
@click.option(
    "--days", "-d", type=int, default=7, help="Number of days to check (default: 7)"
)
def missing(days: int):
    """Find games missing outcome data."""

    console.print(
        f"\n🔍 [bold blue]Missing Game Outcomes[/bold blue] (last {days} days)"
    )

    async def find_missing():
        try:
            # Calculate date range
            from datetime import datetime, timedelta

            end_date = datetime.now()
            start_date = end_date - timedelta(days=days)

            missing_data = await _find_missing_outcomes(start_date, end_date)

            if not missing_data["missing_games"]:
                console.print("✅ [green]All games have outcome data![/green]")
                return

            console.print(
                f"📋 Found {len(missing_data['missing_games'])} games missing outcomes:"
            )

            table = Table(title="🚨 Games Missing Outcomes")
            table.add_column("Game ID", style="cyan")
            table.add_column("Date", style="dim")
            table.add_column("Matchup", style="bold")
            table.add_column("Source", style="yellow")
            table.add_column("Has MLB ID", style="green")

            for game in missing_data["missing_games"]:
                has_mlb_id = "✅" if game.get("mlb_stats_api_game_id") else "❌"
                table.add_row(
                    str(game["id"]),
                    game["game_date"].strftime("%Y-%m-%d")
                    if game["game_date"]
                    else "N/A",
                    f"{game['away_team']} @ {game['home_team']}",
                    game.get("source", "Unknown"),
                    has_mlb_id,
                )

            console.print(table)

            # Show summary
            console.print("\n📊 [bold]Summary:[/bold]")
            console.print(f"  • Total games: {missing_data['total_games']}")
            console.print(f"  • Missing outcomes: {len(missing_data['missing_games'])}")
            console.print(f"  • Missing MLB IDs: {missing_data['missing_mlb_ids']}")
            console.print(f"  • Coverage: {missing_data['coverage_percentage']:.1f}%")

        except Exception as e:
            console.print(f"\n❌ [bold red]Error:[/bold red] {str(e)}")

    asyncio.run(find_missing())


async def _get_action_network_coverage(start_date, end_date):
    """Get outcome coverage for Action Network games."""
    query = """
    SELECT 
        COUNT(*) as total_games,
        COUNT(go.game_id) as games_with_outcomes,
        COUNT(g.mlb_stats_api_game_id) as games_with_mlb_id
    FROM core_betting.games g
    LEFT JOIN core_betting.game_outcomes go ON g.id = go.game_id
    WHERE g.game_date >= %s AND g.game_date <= %s
      AND g.action_network_game_id IS NOT NULL
    """

    async with get_connection() as conn:
        async with conn.cursor() as cursor:
            await cursor.execute(query, [start_date.date(), end_date.date()])
            row = await cursor.fetchone()

            total_games, games_with_outcomes, games_with_mlb_id = row
            coverage_percentage = (
                (games_with_outcomes / total_games * 100) if total_games > 0 else 0
            )

            return {
                "source": "Action Network",
                "total_games": total_games,
                "games_with_outcomes": games_with_outcomes,
                "games_with_mlb_id": games_with_mlb_id,
                "coverage_percentage": coverage_percentage,
                "missing_outcomes": total_games - games_with_outcomes,
            }


async def _get_sbr_coverage(start_date, end_date):
    """Get outcome coverage for SBR games."""
    query = """
    SELECT 
        COUNT(*) as total_games,
        COUNT(go.game_id) as games_with_outcomes,
        COUNT(g.mlb_stats_api_game_id) as games_with_mlb_id
    FROM core_betting.games g
    LEFT JOIN core_betting.game_outcomes go ON g.id = go.game_id
    WHERE g.game_date >= %s AND g.game_date <= %s
      AND g.sportsbookreview_game_id IS NOT NULL
    """

    async with get_connection() as conn:
        async with conn.cursor() as cursor:
            await cursor.execute(query, [start_date.date(), end_date.date()])
            row = await cursor.fetchone()

            total_games, games_with_outcomes, games_with_mlb_id = row
            coverage_percentage = (
                (games_with_outcomes / total_games * 100) if total_games > 0 else 0
            )

            return {
                "source": "SportsbookReview",
                "total_games": total_games,
                "games_with_outcomes": games_with_outcomes,
                "games_with_mlb_id": games_with_mlb_id,
                "coverage_percentage": coverage_percentage,
                "missing_outcomes": total_games - games_with_outcomes,
            }


async def _get_all_sources_coverage(start_date, end_date):
    """Get outcome coverage across all sources."""
    query = """
    SELECT 
        COUNT(*) as total_games,
        COUNT(go.game_id) as games_with_outcomes,
        COUNT(g.mlb_stats_api_game_id) as games_with_mlb_id,
        COUNT(CASE WHEN g.action_network_game_id IS NOT NULL THEN 1 END) as action_network_games,
        COUNT(CASE WHEN g.sportsbookreview_game_id IS NOT NULL THEN 1 END) as sbr_games
    FROM core_betting.games g
    LEFT JOIN core_betting.game_outcomes go ON g.id = go.game_id
    WHERE g.game_date >= %s AND g.game_date <= %s
    """

    async with get_connection() as conn:
        async with conn.cursor() as cursor:
            await cursor.execute(query, [start_date.date(), end_date.date()])
            row = await cursor.fetchone()

            (
                total_games,
                games_with_outcomes,
                games_with_mlb_id,
                action_network_games,
                sbr_games,
            ) = row
            coverage_percentage = (
                (games_with_outcomes / total_games * 100) if total_games > 0 else 0
            )

            return {
                "source": "All Sources",
                "total_games": total_games,
                "games_with_outcomes": games_with_outcomes,
                "games_with_mlb_id": games_with_mlb_id,
                "coverage_percentage": coverage_percentage,
                "missing_outcomes": total_games - games_with_outcomes,
                "action_network_games": action_network_games,
                "sbr_games": sbr_games,
            }


async def _find_missing_outcomes(start_date, end_date):
    """Find games missing outcome data."""
    query = """
    SELECT 
        g.id, g.home_team, g.away_team, g.game_date, g.mlb_stats_api_game_id,
        CASE 
            WHEN g.action_network_game_id IS NOT NULL THEN 'Action Network'
            WHEN g.sportsbookreview_game_id IS NOT NULL THEN 'SBR'
            WHEN g.vsin_game_id IS NOT NULL THEN 'VSIN'
            ELSE 'Unknown'
        END as source
    FROM core_betting.games g
    LEFT JOIN core_betting.game_outcomes go ON g.id = go.game_id
    WHERE g.game_date >= %s AND g.game_date <= %s
      AND go.game_id IS NULL
    ORDER BY g.game_date DESC
    """

    async with get_connection() as conn:
        async with conn.cursor() as cursor:
            await cursor.execute(query, [start_date.date(), end_date.date()])
            rows = await cursor.fetchall()

            missing_games = []
            missing_mlb_ids = 0

            for row in rows:
                game_data = {
                    "id": row[0],
                    "home_team": row[1],
                    "away_team": row[2],
                    "game_date": row[3],
                    "mlb_stats_api_game_id": row[4],
                    "source": row[5],
                }
                missing_games.append(game_data)

                if not row[4]:  # No MLB Stats API ID
                    missing_mlb_ids += 1

            # Get total games for coverage calculation
            total_query = """
            SELECT COUNT(*) FROM core_betting.games 
            WHERE game_date >= %s AND game_date <= %s
            """
            await cursor.execute(total_query, [start_date.date(), end_date.date()])
            total_games = (await cursor.fetchone())[0]

            coverage_percentage = (
                ((total_games - len(missing_games)) / total_games * 100)
                if total_games > 0
                else 0
            )

            return {
                "missing_games": missing_games,
                "missing_mlb_ids": missing_mlb_ids,
                "total_games": total_games,
                "coverage_percentage": coverage_percentage,
            }


async def _run_outcome_verification(games_type, start_date, end_date, fix_missing):
    """Run outcome verification checks."""
    # Placeholder for comprehensive verification logic
    return {
        "verified_games": 0,
        "consistency_errors": [],
        "fixed_ids": 0,
        "remaining_issues": 0,
    }


def _display_coverage_table(coverage_data, date_range):
    """Display coverage results in a table."""
    table = Table(title=f"📊 Outcome Coverage - {date_range}")
    table.add_column("Source", style="cyan")
    table.add_column("Total Games", style="bold")
    table.add_column("With Outcomes", style="green")
    table.add_column("Coverage %", style="yellow")
    table.add_column("Missing MLB IDs", style="red")

    table.add_row(
        coverage_data["source"],
        str(coverage_data["total_games"]),
        str(coverage_data["games_with_outcomes"]),
        f"{coverage_data['coverage_percentage']:.1f}%",
        str(coverage_data["total_games"] - coverage_data["games_with_mlb_id"]),
    )

    console.print(table)


def _display_missing_games(missing_games):
    """Display missing games table."""
    # Implementation would show missing games in a formatted table
    pass


def _display_verification_results(results):
    """Display verification results."""
    # Implementation would show verification results
    pass


def _display_resolution_stats(stats: dict, days: int):
    """Display game ID resolution statistics."""
    console.print("\n📊 [bold]Game ID Resolution Statistics[/bold]")

    if not stats:
        console.print("❌ No statistics available")
        return

    # Main statistics table
    table = Table(title=f"📈 Coverage Analysis - Last {days} Days")
    table.add_column("Metric", style="cyan")
    table.add_column("Count", style="bold")
    table.add_column("Percentage", style="yellow")

    total_games = stats.get("total_games", 0)
    games_with_mlb_id = stats.get("games_with_mlb_id", 0)
    missing_mlb_ids = stats.get("missing_mlb_ids", 0)
    resolvable_missing = stats.get("resolvable_missing", 0)

    table.add_row("Total Games", str(total_games), "100.0%")
    table.add_row(
        "With MLB Stats API ID",
        str(games_with_mlb_id),
        f"{stats.get('coverage_percentage', 0):.1f}%",
    )
    table.add_row(
        "Missing MLB Stats API ID",
        str(missing_mlb_ids),
        f"{100 - stats.get('coverage_percentage', 0):.1f}%",
    )
    table.add_row(
        "Resolvable Missing",
        str(resolvable_missing),
        f"{stats.get('improvement_potential', 0):.1f}% potential",
    )

    console.print(table)

    # Source breakdown
    source_breakdown = stats.get("source_breakdown", {})
    if source_breakdown:
        console.print("\n📋 [bold]Source Breakdown:[/bold]")
        for source, count in source_breakdown.items():
            console.print(f"  • {source.replace('_', ' ').title()}: {count} games")

    # Improvement potential
    improvement = stats.get("improvement_potential", 0)
    if improvement > 0:
        console.print("\n💡 [bold green]Improvement Potential:[/bold green]")
        console.print(f"  • {improvement:.1f}% coverage increase possible")
        console.print(f"  • {resolvable_missing} games can be resolved")
        console.print(
            f"  • Potential coverage: {stats.get('potential_coverage_percentage', 0):.1f}%"
        )
    else:
        console.print("\n✅ [bold green]Full Coverage Achieved![/bold green]")


def _display_resolution_results(results: dict, dry_run: bool):
    """Display game ID resolution results."""
    mode = "DRY RUN" if dry_run else "LIVE"
    console.print(f"\n📋 [bold]Resolution Results ({mode})[/bold]")

    # Summary statistics
    summary_text = f"""
📊 Processed: {results["processed_games"]} games
✅ Resolved: {results["resolved_games"]} IDs
❌ Failed: {results["failed_resolutions"]} resolutions
⏭️  Skipped: {results["skipped_games"]} games
🚨 Errors: {len(results["errors"])}
"""

    console.print(Panel(summary_text.strip(), title="📈 Summary", border_style="blue"))

    # Show successful resolutions
    resolutions = results.get("resolutions", [])
    if resolutions:
        console.print(
            f"\n✅ [bold green]Successfully Resolved ({len(resolutions)}):[/bold green]"
        )

        resolution_table = Table()
        resolution_table.add_column("Game ID", style="cyan")
        resolution_table.add_column("Matchup", style="bold")
        resolution_table.add_column("MLB Game ID", style="green")
        resolution_table.add_column("Confidence", style="yellow")
        resolution_table.add_column("Source", style="dim")

        for resolution in resolutions[:10]:  # Show first 10
            matchup = f"{resolution['away_team']} @ {resolution['home_team']}"
            resolution_table.add_row(
                str(resolution["game_id"]),
                matchup,
                resolution["mlb_game_id"],
                resolution["confidence"],
                resolution["source"].replace("_", " ").title(),
            )

        console.print(resolution_table)

        if len(resolutions) > 10:
            console.print(f"  ... and {len(resolutions) - 10} more")

    # Show errors if any
    errors = results.get("errors", [])
    if errors:
        console.print(f"\n❌ [bold red]Errors ({len(errors)}):[/bold red]")
        for error in errors[:5]:  # Show first 5 errors
            console.print(f"  • {error}")
        if len(errors) > 5:
            console.print(f"  ... and {len(errors) - 5} more errors")

    # Next steps
    if not dry_run and results["resolved_games"] > 0:
        console.print("\n🎯 [bold]Next Steps:[/bold]")
        console.print("  • Run outcome checking to update game results")
        console.print(
            "  • Command: uv run -m src.interfaces.cli outcomes check --days 30"
        )
    elif dry_run and results["resolved_games"] > 0:
        console.print("\n🎯 [bold]To Apply Changes:[/bold]")
        console.print("  • Run without --dry-run flag to update database")
        console.print(
            "  • Command: uv run -m src.interfaces.cli outcomes resolve-ids --days 30"
        )


def _display_results(results: dict, verbose: bool):
    """Display the results of a game outcome check."""

    # Create summary panel
    summary_text = f"""
📊 Processed: {results["processed_games"]} games
✅ Updated: {results["updated_outcomes"]} outcomes  
⏳ Skipped: {results["skipped_games"]} games (not completed)
❌ Errors: {len(results["errors"])}
📅 Date Range: {results["date_range"][0]} to {results["date_range"][1]}
"""

    console.print(
        Panel(summary_text.strip(), title="📋 Results Summary", border_style="blue")
    )

    if results["errors"] and verbose:
        console.print("\n❌ [bold red]Errors:[/bold red]")
        for error in results["errors"]:
            console.print(f"  • {error}")


def _display_outcomes_table(outcomes: list):
    """Display game outcomes in a table format."""

    if not outcomes:
        console.print("📭 No recent game outcomes found")
        return

    table = Table(title="🏁 Recent Game Outcomes")
    table.add_column("Date", style="dim")
    table.add_column("Matchup", style="bold")
    table.add_column("Score", style="green")
    table.add_column("Winner", style="bold green")
    table.add_column("Over/Under", style="cyan")
    table.add_column("Spread", style="yellow")

    for outcome in outcomes:
        # Format date
        game_date = outcome["game_date"]
        if isinstance(game_date, str):
            date_str = game_date.split("T")[0]  # Take date part only
        else:
            date_str = game_date.strftime("%Y-%m-%d")

        # Format matchup
        matchup = f"{outcome['away_team']} @ {outcome['home_team']}"

        # Format score
        score = f"{outcome['away_score']}-{outcome['home_score']}"

        # Format winner
        winner = outcome["home_team"] if outcome["home_win"] else outcome["away_team"]

        # Format over/under
        if outcome["over"] is not None:
            total = outcome["home_score"] + outcome["away_score"]
            over_under = f"{'Over' if outcome['over'] else 'Under'} {outcome['total_line']} (Total: {total})"
        else:
            over_under = "N/A"

        # Format spread
        if outcome["home_cover_spread"] is not None:
            spread_result = f"{outcome['home_team']} {'✓' if outcome['home_cover_spread'] else '✗'} ({outcome['home_spread_line']})"
        else:
            spread_result = "N/A"

        table.add_row(date_str, matchup, score, winner, over_under, spread_result)

    console.print(table)


if __name__ == "__main__":
    outcomes()
