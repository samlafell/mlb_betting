"""
CLI commands for enhanced movement analysis and betting intelligence.
"""

import asyncio
import json
from datetime import datetime
from pathlib import Path

import click
from rich.console import Console
from rich.panel import Panel
from rich.table import Table

from src.analysis.processors.movement_analyzer import MovementAnalyzer
from src.data.models.unified.movement_analysis import MovementAnalysisReport

console = Console()


@click.group()
def movement():
    """Enhanced movement analysis and betting intelligence commands."""
    pass


@movement.command()
@click.option(
    "--input-file", "-i", required=True, help="Path to historical data JSON file"
)
@click.option("--output-file", "-o", help="Path to save detailed analysis report")
@click.option("--game-id", type=int, help="Analyze specific game ID only")
@click.option(
    "--min-movements", type=int, default=50, help="Minimum movements to analyze game"
)
@click.option("--show-details", is_flag=True, help="Show detailed movement breakdown")
@click.option("--rlm-only", is_flag=True, help="Show only games with RLM indicators")
@click.option("--steam-only", is_flag=True, help="Show only games with steam moves")
def analyze(
    input_file: str,
    output_file: str | None,
    game_id: int | None,
    min_movements: int,
    show_details: bool,
    rlm_only: bool,
    steam_only: bool,
):
    """Perform detailed movement analysis with RLM and steam detection."""
    return asyncio.run(
        _analyze_async(
            input_file,
            output_file,
            game_id,
            min_movements,
            show_details,
            rlm_only,
            steam_only,
        )
    )


async def _analyze_async(
    input_file: str,
    output_file: str | None,
    game_id: int | None,
    min_movements: int,
    show_details: bool,
    rlm_only: bool,
    steam_only: bool,
):
    """Perform detailed movement analysis with RLM and steam detection."""

    console.print("[bold blue]🔍 Enhanced Movement Analysis[/bold blue]")
    console.print(f"Loading data from: {input_file}")

    # Load historical data
    try:
        with open(input_file) as f:
            data = json.load(f)
    except FileNotFoundError:
        console.print(f"[red]❌ File not found: {input_file}[/red]")
        return
    except json.JSONDecodeError:
        console.print(f"[red]❌ Invalid JSON file: {input_file}[/red]")
        return

    historical_data = data.get("historical_data", [])
    console.print(f"Found {len(historical_data)} games to analyze")

    # Initialize analyzer
    analyzer = MovementAnalyzer()

    # Filter games
    games_to_analyze = []
    for game_data in historical_data:
        if game_id and game_data.get("game_id") != game_id:
            continue
        games_to_analyze.append(game_data)

    if not games_to_analyze:
        console.print("[yellow]⚠️  No games match the criteria[/yellow]")
        return

    console.print(f"Analyzing {len(games_to_analyze)} games...")

    # Analyze each game
    game_analyses = []
    with console.status("[bold green]Analyzing movements...") as status:
        for i, game_data in enumerate(games_to_analyze):
            status.update(f"Analyzing game {i + 1}/{len(games_to_analyze)}")

            try:
                analysis = await analyzer.analyze_game_movements(game_data)

                # Apply filters
                if min_movements and analysis.total_movements < min_movements:
                    continue
                if rlm_only and not analysis.rlm_indicators:
                    continue
                if steam_only and not any(
                    c.steam_move_detected for c in analysis.cross_book_movements
                ):
                    continue

                game_analyses.append(analysis)

            except Exception as e:
                console.print(
                    f"[red]❌ Error analyzing game {game_data.get('game_id', 'unknown')}: {e}[/red]"
                )

    if not game_analyses:
        console.print("[yellow]⚠️  No games meet the analysis criteria[/yellow]")
        return

    # Create comprehensive report
    report = MovementAnalysisReport(
        analysis_timestamp=datetime.now(),
        total_games_analyzed=len(game_analyses),
        games_with_rlm=len([g for g in game_analyses if g.rlm_indicators]),
        games_with_steam_moves=len(
            [
                g
                for g in game_analyses
                if any(c.steam_move_detected for c in g.cross_book_movements)
            ]
        ),
        games_with_arbitrage=len(
            [g for g in game_analyses if g.arbitrage_opportunities]
        ),
        game_analyses=game_analyses,
    )

    # Display results
    await _display_analysis_results(report, show_details)

    # Save detailed report
    if output_file:
        output_path = Path(output_file)
        output_path.parent.mkdir(parents=True, exist_ok=True)

        with open(output_path, "w") as f:
            json.dump(report.dict(), f, indent=2, default=str)

        console.print(f"[green]✅ Detailed report saved to: {output_file}[/green]")


@movement.command()
@click.option(
    "--input-file", "-i", required=True, help="Path to historical data JSON file"
)
@click.option(
    "--min-rlm-strength",
    type=click.Choice(["weak", "moderate", "strong"]),
    default="moderate",
    help="Minimum RLM strength to display",
)
@click.option(
    "--market-type",
    type=click.Choice(["moneyline", "spread", "total"]),
    help="Filter by market type",
)
@click.option("--sportsbook", help="Filter by sportsbook ID")
def rlm(
    input_file: str,
    min_rlm_strength: str,
    market_type: str | None,
    sportsbook: str | None,
):
    """Detect and analyze Reverse Line Movement (RLM) opportunities."""
    return asyncio.run(
        _rlm_async(input_file, min_rlm_strength, market_type, sportsbook)
    )


async def _rlm_async(
    input_file: str,
    min_rlm_strength: str,
    market_type: str | None,
    sportsbook: str | None,
):
    """Detect and analyze Reverse Line Movement (RLM) opportunities."""

    console.print("[bold red]🔄 Reverse Line Movement Analysis[/bold red]")
    console.print(f"Minimum RLM strength: {min_rlm_strength}")

    # Load and analyze data
    try:
        with open(input_file) as f:
            data = json.load(f)
    except FileNotFoundError:
        console.print(f"[red]❌ File not found: {input_file}[/red]")
        return

    analyzer = MovementAnalyzer()
    historical_data = data.get("historical_data", [])

    rlm_opportunities = []

    with console.status("[bold green]Detecting RLM patterns...") as status:
        for i, game_data in enumerate(historical_data):
            status.update(f"Analyzing game {i + 1}/{len(historical_data)}")

            try:
                analysis = await analyzer.analyze_game_movements(game_data)

                # Filter RLM indicators
                for rlm in analysis.rlm_indicators:
                    # Apply filters
                    if min_rlm_strength == "strong" and rlm.rlm_strength != "strong":
                        continue
                    if min_rlm_strength == "moderate" and rlm.rlm_strength == "weak":
                        continue
                    if market_type and rlm.market_type.value != market_type:
                        continue
                    if sportsbook and rlm.sportsbook_id != sportsbook:
                        continue

                    rlm_opportunities.append({"game": analysis, "rlm": rlm})

            except Exception as e:
                console.print(
                    f"[red]❌ Error analyzing game {game_data.get('game_id', 'unknown')}: {e}[/red]"
                )

    if not rlm_opportunities:
        console.print(
            "[yellow]⚠️  No RLM opportunities found with the specified criteria[/yellow]"
        )
        return

    # Display RLM opportunities
    await _display_rlm_opportunities(rlm_opportunities, analyzer)


@movement.command()
@click.option(
    "--input-file", "-i", required=True, help="Path to historical data JSON file"
)
@click.option(
    "--min-books", type=int, default=3, help="Minimum number of books for steam move"
)
@click.option(
    "--market-type",
    type=click.Choice(["moneyline", "spread", "total"]),
    help="Filter by market type",
)
def steam(input_file: str, min_books: int, market_type: str | None):
    """Detect steam moves across multiple sportsbooks."""
    return asyncio.run(_steam_async(input_file, min_books, market_type))


async def _steam_async(input_file: str, min_books: int, market_type: str | None):
    """Detect steam moves across multiple sportsbooks."""

    console.print("[bold green]🚂 Steam Move Detection[/bold green]")
    console.print(f"Minimum books required: {min_books}")

    # Load and analyze data
    try:
        with open(input_file) as f:
            data = json.load(f)
    except FileNotFoundError:
        console.print(f"[red]❌ File not found: {input_file}[/red]")
        return

    analyzer = MovementAnalyzer()
    historical_data = data.get("historical_data", [])

    steam_moves = []

    with console.status("[bold green]Detecting steam moves...") as status:
        for i, game_data in enumerate(historical_data):
            status.update(f"Analyzing game {i + 1}/{len(historical_data)}")

            try:
                analysis = await analyzer.analyze_game_movements(game_data)

                # Filter steam moves
                for steam_move in analysis.cross_book_movements:
                    if not steam_move.steam_move_detected:
                        continue
                    if len(steam_move.participating_books) < min_books:
                        continue
                    if market_type and steam_move.market_type.value != market_type:
                        continue

                    steam_moves.append({"game": analysis, "steam": steam_move})

            except Exception as e:
                console.print(
                    f"[red]❌ Error analyzing game {game_data.get('game_id', 'unknown')}: {e}[/red]"
                )

    if not steam_moves:
        console.print(
            "[yellow]⚠️  No steam moves found with the specified criteria[/yellow]"
        )
        return

    # Display steam moves
    await _display_steam_moves(steam_moves, analyzer)


async def _display_analysis_results(report: MovementAnalysisReport, show_details: bool):
    """Display comprehensive analysis results."""

    # Summary panel
    summary_text = f"""
    [bold]Total Games Analyzed:[/bold] {report.total_games_analyzed}
    [bold]Games with RLM:[/bold] {report.games_with_rlm}
    [bold]Games with Steam Moves:[/bold] {report.games_with_steam_moves}
    [bold]Games with Arbitrage:[/bold] {report.games_with_arbitrage}
    [bold]Total Movements:[/bold] {report.total_movements:,}
    """

    console.print(Panel(summary_text, title="📊 Analysis Summary", border_style="blue"))

    # Top opportunities
    if report.games_with_rlm > 0:
        console.print("\n[bold red]🔥 Top RLM Opportunities[/bold red]")

        # Sort games by RLM count
        rlm_games = sorted(
            [g for g in report.game_analyses if g.rlm_indicators],
            key=lambda x: len(x.rlm_indicators),
            reverse=True,
        )[:5]

        for game in rlm_games:
            strong_rlm = [r for r in game.rlm_indicators if r.rlm_strength == "strong"]
            moderate_rlm = [
                r for r in game.rlm_indicators if r.rlm_strength == "moderate"
            ]

            console.print(f"  [bold]{game.away_team} @ {game.home_team}[/bold]")
            console.print(
                f"    Strong RLM: {len(strong_rlm)}, Moderate RLM: {len(moderate_rlm)}"
            )

            if show_details:
                for rlm in game.rlm_indicators[:3]:  # Show top 3
                    book_name = _get_book_name(rlm.sportsbook_id)
                    console.print(
                        f"    - {book_name} {rlm.market_type.value}: {rlm.rlm_strength} RLM"
                    )

    # Steam moves
    if report.games_with_steam_moves > 0:
        console.print("\n[bold green]🚂 Steam Move Alerts[/bold green]")

        steam_games = [
            g
            for g in report.game_analyses
            if any(c.steam_move_detected for c in g.cross_book_movements)
        ]

        for game in steam_games[:5]:
            steam_moves = [
                c for c in game.cross_book_movements if c.steam_move_detected
            ]

            console.print(f"  [bold]{game.away_team} @ {game.home_team}[/bold]")
            console.print(f"    Steam moves detected: {len(steam_moves)}")

            if show_details:
                for steam in steam_moves[:2]:  # Show top 2
                    console.print(
                        f"    - {steam.market_type.value}: {len(steam.participating_books)} books, {steam.consensus_strength} consensus"
                    )

    # Recommendations
    if show_details:
        console.print("\n[bold yellow]💡 Recommended Actions[/bold yellow]")

        for game in report.game_analyses[:3]:  # Top 3 games
            if game.recommended_actions:
                console.print(f"  [bold]{game.away_team} @ {game.home_team}[/bold]")
                for action in game.recommended_actions:
                    console.print(f"    - {action}")


async def _display_rlm_opportunities(opportunities, analyzer):
    """Display RLM opportunities in a detailed table."""

    table = Table(title="🔄 Reverse Line Movement Opportunities")
    table.add_column("Game", style="cyan")
    table.add_column("Sportsbook", style="magenta")
    table.add_column("Market", style="green")
    table.add_column("RLM Strength", style="red")
    table.add_column("Public %", style="blue")
    table.add_column("Line Movement", style="yellow")

    for opp in opportunities:
        game = opp["game"]
        rlm = opp["rlm"]

        game_name = f"{game.away_team} @ {game.home_team}"
        book_name = analyzer.sportsbook_names.get(rlm.sportsbook_id, rlm.sportsbook_id)

        table.add_row(
            game_name,
            book_name,
            rlm.market_type.value,
            rlm.rlm_strength or "N/A",
            f"{rlm.public_percentage}%" if rlm.public_percentage else "N/A",
            f"{rlm.line_movement_amount}" if rlm.line_movement_amount else "N/A",
        )

    console.print(table)


async def _display_steam_moves(steam_moves, analyzer):
    """Display steam moves in a detailed table."""

    table = Table(title="🚂 Steam Move Detection")
    table.add_column("Game", style="cyan")
    table.add_column("Market", style="green")
    table.add_column("Books", style="magenta")
    table.add_column("Consensus", style="red")
    table.add_column("Direction", style="blue")
    table.add_column("Avg Movement", style="yellow")

    for steam_data in steam_moves:
        game = steam_data["game"]
        steam = steam_data["steam"]

        game_name = f"{game.away_team} @ {game.home_team}"
        book_names = [
            analyzer.sportsbook_names.get(book_id, book_id)
            for book_id in steam.participating_books
        ]

        table.add_row(
            game_name,
            steam.market_type.value,
            f"{len(steam.participating_books)} ({', '.join(book_names[:3])}{'...' if len(book_names) > 3 else ''})",
            steam.consensus_strength or "N/A",
            steam.consensus_direction.value if steam.consensus_direction else "N/A",
            f"{steam.average_movement}" if steam.average_movement else "N/A",
        )

    console.print(table)


def _get_book_name(sportsbook_id: str) -> str:
    """Get friendly sportsbook name."""
    names = {
        "15": "DraftKings",
        "30": "FanDuel",
        "68": "Caesars",
        "69": "BetMGM",
        "71": "PointsBet",
        "75": "Barstool",
    }
    return names.get(sportsbook_id, sportsbook_id)
