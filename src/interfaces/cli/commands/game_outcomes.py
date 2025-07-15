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
