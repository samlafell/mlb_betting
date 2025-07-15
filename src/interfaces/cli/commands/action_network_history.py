"""
CLI command for extracting Action Network historical line movement data.

This command processes JSON files containing Action Network game data
and extracts historical line movement information from history URLs.
"""

import asyncio
import json
from datetime import datetime
from pathlib import Path

import click
import structlog

from ....services.data.unified_data_service import get_unified_data_service

logger = structlog.get_logger(__name__)


@click.command("extract-action-network-history")
@click.option(
    "--input-file",
    "-i",
    type=click.Path(exists=True, path_type=Path),
    required=True,
    help="JSON file containing Action Network game data with history URLs",
)
@click.option(
    "--output-file",
    "-o",
    type=click.Path(path_type=Path),
    help="Output file to save extracted historical data (optional)",
)
@click.option(
    "--max-games",
    "-m",
    type=int,
    default=None,
    help="Maximum number of games to process (for testing)",
)
@click.option(
    "--dry-run",
    "-d",
    is_flag=True,
    help="Show what would be processed without actually extracting data",
)
@click.option("--verbose", "-v", is_flag=True, help="Enable verbose logging")
async def extract_action_network_history(
    input_file: Path,
    output_file: Path | None,
    max_games: int | None,
    dry_run: bool,
    verbose: bool,
):
    """
    Extract historical line movement data from Action Network.

    This command reads a JSON file containing Action Network game data
    and extracts historical line movement data from the history URLs.

    Examples:
        # Extract from today's games
        uv run python -m src.interfaces.cli extract-action-network-history -i output/games_today.json

        # Extract with output file
        uv run python -m src.interfaces.cli extract-action-network-history -i output/games_today.json -o output/history_data.json

        # Test with limited games
        uv run python -m src.interfaces.cli extract-action-network-history -i output/games_today.json -m 5 --dry-run
    """
    # Configure logging
    if verbose:
        structlog.configure(
            processors=[
                structlog.stdlib.filter_by_level,
                structlog.stdlib.add_logger_name,
                structlog.stdlib.add_log_level,
                structlog.stdlib.PositionalArgumentsFormatter(),
                structlog.dev.ConsoleRenderer(colors=True),
            ],
            context_class=dict,
            logger_factory=structlog.stdlib.LoggerFactory(),
            wrapper_class=structlog.stdlib.BoundLogger,
            cache_logger_on_first_use=True,
        )

    click.echo("🏈 Action Network Historical Line Movement Extractor")
    click.echo("=" * 60)

    try:
        # Read and analyze input file
        click.echo(f"📁 Reading input file: {input_file}")

        with open(input_file) as f:
            game_data = json.load(f)

        # Extract games with history URLs
        games_with_history = []
        total_games = len(game_data.get("games", []))

        for game in game_data.get("games", []):
            if "history_url" in game and game["history_url"]:
                # Convert datetime if needed
                game_datetime = game.get("start_time")
                if isinstance(game_datetime, str):
                    game_datetime = datetime.fromisoformat(
                        game_datetime.replace("Z", "+00:00")
                    )

                game_info = {
                    "game_id": game.get("game_id"),
                    "home_team": game.get("home_team"),
                    "away_team": game.get("away_team"),
                    "game_datetime": game_datetime,
                    "history_url": game.get("history_url"),
                }
                games_with_history.append(game_info)

        click.echo(
            f"📊 Found {len(games_with_history)} games with history URLs out of {total_games} total games"
        )

        if not games_with_history:
            click.echo("❌ No games with history URLs found in the input file")
            return

        # Apply max games limit if specified
        if max_games and max_games < len(games_with_history):
            games_with_history = games_with_history[:max_games]
            click.echo(f"🔢 Limited to {max_games} games for processing")

        # Show what would be processed
        if dry_run:
            click.echo("\n🔍 DRY RUN - Games that would be processed:")
            for i, game in enumerate(games_with_history, 1):
                click.echo(
                    f"  {i}. {game['away_team']} @ {game['home_team']} (ID: {game['game_id']})"
                )
            click.echo(f"\n✅ Would process {len(games_with_history)} games")
            return

        # Initialize unified data service
        click.echo("🚀 Initializing data service...")
        data_service = get_unified_data_service()

        # Extract historical data
        click.echo(
            f"📈 Extracting historical line movement data for {len(games_with_history)} games..."
        )

        with click.progressbar(
            length=len(games_with_history), label="Extracting histories"
        ) as bar:
            # Process games in batches to avoid overwhelming the API
            batch_size = 5
            all_historical_data = []

            for i in range(0, len(games_with_history), batch_size):
                batch = games_with_history[i : i + batch_size]

                # Extract batch
                batch_results = (
                    await data_service.collect_multiple_action_network_histories(batch)
                )
                all_historical_data.extend(batch_results)

                # Update progress
                bar.update(len(batch))

                # Small delay between batches to be respectful
                if i + batch_size < len(games_with_history):
                    await asyncio.sleep(1)

        # Report results
        successful_extractions = len(all_historical_data)
        failed_extractions = len(games_with_history) - successful_extractions

        click.echo("\n📊 Extraction Results:")
        click.echo(f"  ✅ Successful: {successful_extractions}")
        click.echo(f"  ❌ Failed: {failed_extractions}")

        if successful_extractions > 0:
            # Calculate summary statistics
            total_entries = sum(data.total_entries for data in all_historical_data)
            pregame_entries = sum(data.pregame_entries for data in all_historical_data)
            live_entries = sum(data.live_entries for data in all_historical_data)

            click.echo("\n📈 Historical Data Summary:")
            click.echo(f"  📊 Total entries: {total_entries}")
            click.echo(f"  🎯 Pregame entries: {pregame_entries}")
            click.echo(f"  🔴 Live entries: {live_entries}")

            # Save to output file
            if output_file:
                click.echo(f"\n💾 Saving historical data to: {output_file}")
                success = await data_service.save_historical_data_to_json(
                    all_historical_data, str(output_file)
                )
                if success:
                    click.echo("✅ Historical data saved successfully")
                else:
                    click.echo("❌ Failed to save historical data")
            else:
                # Generate default output filename
                timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
                default_output = Path(f"output/action_network_history_{timestamp}.json")

                click.echo(f"\n💾 Saving historical data to: {default_output}")
                success = await data_service.save_historical_data_to_json(
                    all_historical_data, str(default_output)
                )
                if success:
                    click.echo("✅ Historical data saved successfully")
                else:
                    click.echo("❌ Failed to save historical data")

        # Show service statistics
        stats = data_service.get_service_stats()
        click.echo("\n📊 Service Statistics:")
        click.echo(f"  🎯 Success rate: {stats['success_rate']:.1%}")
        click.echo(f"  📈 Total operations: {stats['total_operations']}")
        click.echo(f"  ✅ Successful: {stats['successful_operations']}")
        click.echo(f"  ❌ Failed: {stats['failed_operations']}")

        # Cleanup
        await data_service.cleanup()

        click.echo("\n🎉 Action Network history extraction completed!")

    except Exception as e:
        click.echo(f"❌ Error during extraction: {str(e)}")
        logger.error("Action Network history extraction failed", error=str(e))
        raise click.ClickException(f"Extraction failed: {str(e)}")


@click.command("analyze-action-network-history")
@click.option(
    "--input-file",
    "-i",
    type=click.Path(exists=True, path_type=Path),
    required=True,
    help="JSON file containing extracted historical data",
)
@click.option(
    "--output-report",
    "-r",
    type=click.Path(path_type=Path),
    help="Output file for analysis report",
)
@click.option("--game-id", "-g", type=int, help="Analyze specific game ID only")
def analyze_action_network_history(
    input_file: Path, output_report: Path | None, game_id: int | None
):
    """
    Analyze extracted Action Network historical line movement data.

    This command analyzes previously extracted historical data and generates
    insights about line movement patterns.

    Examples:
        # Analyze all games
        uv run python -m src.interfaces.cli analyze-action-network-history -i output/history_data.json

        # Analyze specific game
        uv run python -m src.interfaces.cli analyze-action-network-history -i output/history_data.json -g 259124

        # Generate analysis report
        uv run python -m src.interfaces.cli analyze-action-network-history -i output/history_data.json -r output/analysis_report.json
    """
    click.echo("📊 Action Network Historical Data Analyzer")
    click.echo("=" * 60)

    try:
        # Read historical data
        click.echo(f"📁 Reading historical data: {input_file}")

        with open(input_file) as f:
            historical_data = json.load(f)

        games_data = historical_data.get("historical_data", [])
        click.echo(f"📊 Found {len(games_data)} games with historical data")

        if not games_data:
            click.echo("❌ No historical data found in the input file")
            return

        # Filter by game ID if specified
        if game_id:
            games_data = [game for game in games_data if game.get("game_id") == game_id]
            if not games_data:
                click.echo(f"❌ No data found for game ID: {game_id}")
                return
            click.echo(f"🎯 Analyzing game ID: {game_id}")

        # Analyze line movement patterns
        analysis_results = {
            "analyzed_at": datetime.now().isoformat(),
            "total_games": len(games_data),
            "games_analyzed": [],
        }

        total_line_movements = 0
        total_significant_movements = 0

        for game in games_data:
            game_analysis = {
                "game_id": game.get("game_id"),
                "home_team": game.get("home_team"),
                "away_team": game.get("away_team"),
                "game_datetime": game.get("game_datetime"),
                "total_entries": game.get("total_entries", 0),
                "pregame_entries": game.get("pregame_entries", 0),
                "live_entries": game.get("live_entries", 0),
            }

            # Analyze line movement summary
            line_summary = game.get("line_movement_summary", {})
            if line_summary:
                game_analysis["line_movement_summary"] = line_summary

                # Count movements
                for market_type in ["moneyline", "spread", "total"]:
                    market_data = line_summary.get(market_type, {})
                    total_line_movements += market_data.get("movements", 0)
                    total_significant_movements += market_data.get(
                        "significant_moves", 0
                    )

            analysis_results["games_analyzed"].append(game_analysis)

            # Display game summary
            click.echo(
                f"\n🎮 {game['away_team']} @ {game['home_team']} (ID: {game['game_id']})"
            )
            click.echo(f"  📊 Total entries: {game.get('total_entries', 0)}")
            click.echo(
                f"  🎯 Pregame: {game.get('pregame_entries', 0)}, Live: {game.get('live_entries', 0)}"
            )

            if line_summary:
                click.echo("  📈 Line movements:")
                for market_type in ["moneyline", "spread", "total"]:
                    market_data = line_summary.get(market_type, {})
                    movements = market_data.get("movements", 0)
                    significant = market_data.get("significant_moves", 0)
                    click.echo(
                        f"    {market_type.title()}: {movements} moves ({significant} significant)"
                    )

        # Overall summary
        analysis_results.update(
            {
                "total_line_movements": total_line_movements,
                "total_significant_movements": total_significant_movements,
                "significant_movement_rate": (
                    (total_significant_movements / total_line_movements * 100)
                    if total_line_movements > 0
                    else 0
                ),
            }
        )

        click.echo("\n📊 Overall Analysis Summary:")
        click.echo(f"  🎮 Games analyzed: {len(games_data)}")
        click.echo(f"  📈 Total line movements: {total_line_movements}")
        click.echo(f"  ⚡ Significant movements: {total_significant_movements}")
        click.echo(
            f"  📊 Significant rate: {analysis_results['significant_movement_rate']:.1f}%"
        )

        # Save analysis report
        if output_report:
            click.echo(f"\n💾 Saving analysis report to: {output_report}")
            output_report.parent.mkdir(parents=True, exist_ok=True)

            with open(output_report, "w") as f:
                json.dump(analysis_results, f, indent=2, default=str)

            click.echo("✅ Analysis report saved successfully")

        click.echo("\n🎉 Analysis completed!")

    except Exception as e:
        click.echo(f"❌ Error during analysis: {str(e)}")
        logger.error("Action Network history analysis failed", error=str(e))
        raise click.ClickException(f"Analysis failed: {str(e)}")


# Register commands with the CLI
def register_commands(cli_group):
    """Register Action Network history commands with the CLI."""
    cli_group.add_command(extract_action_network_history)
    cli_group.add_command(analyze_action_network_history)
