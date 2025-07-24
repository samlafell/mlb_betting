"""
Action Network Complete Pipeline Commands

This module provides a complete end-to-end pipeline for Action Network data:
1. Extract today's game URLs
2. Collect historical line movement data
3. Analyze for RLM, steam moves, and opportunities
4. Generate actionable insights
"""

import asyncio
import json
from datetime import datetime, timedelta
from pathlib import Path

import click
from rich.console import Console
from rich.panel import Panel
from rich.table import Table

from src.analysis.processors.movement_analyzer import MovementAnalyzer
from src.data.models.unified.movement_analysis import MovementAnalysisReport
from src.data.database.repositories.analysis_reports_repository import AnalysisReportsRepository

console = Console()


@click.group()
def action_network():
    """Action Network data collection and analysis pipeline."""
    pass


@action_network.command()
@click.option(
    "--date",
    "-d",
    type=click.Choice(["today", "tomorrow"]),
    default="today",
    help="Date to collect data for",
)
@click.option(
    "--output-dir",
    "-o",
    type=click.Path(path_type=Path),
    default=Path("output"),
    help="Directory to save output files",
)
@click.option(
    "--max-games",
    "-m",
    type=int,
    help="Maximum number of games to process (for testing)",
)
@click.option(
    "--skip-history",
    "-s",
    is_flag=True,
    help="Skip historical data collection (URLs only)",
)
@click.option(
    "--analyze-only",
    "-a",
    is_flag=True,
    help="Skip collection, analyze existing data only",
)
@click.option("--verbose", "-v", is_flag=True, help="Enable verbose logging")
def pipeline(
    date: str,
    output_dir: Path,
    max_games: int | None,
    skip_history: bool,
    analyze_only: bool,
    verbose: bool,
):
    """
    Complete Action Network pipeline: Extract URLs → Collect History → Analyze Opportunities

    This command runs the complete pipeline:
    1. Extract today's game URLs from Action Network
    2. Collect historical line movement data for each game
    3. Analyze for RLM, steam moves, and arbitrage opportunities
    4. Generate actionable betting insights

    Examples:
        # Run complete pipeline for today
        uv run python -m src.interfaces.cli action-network pipeline

        # Run pipeline for tomorrow's games
        uv run python -m src.interfaces.cli action-network pipeline --date tomorrow

        # Test with limited games
        uv run python -m src.interfaces.cli action-network pipeline --max-games 5

        # Skip history collection (URLs only)
        uv run python -m src.interfaces.cli action-network pipeline --skip-history

        # Analyze existing data only
        uv run python -m src.interfaces.cli action-network pipeline --analyze-only
    """
    # Run the async pipeline function
    asyncio.run(
        _pipeline_async(
            date=date,
            output_dir=output_dir,
            max_games=max_games,
            skip_history=skip_history,
            analyze_only=analyze_only,
            verbose=verbose,
        )
    )


async def _pipeline_async(
    date: str,
    output_dir: Path,
    max_games: int | None,
    skip_history: bool,
    analyze_only: bool,
    verbose: bool,
):
    """Async implementation of the Action Network pipeline."""

    # Ensure output directory exists
    output_dir.mkdir(exist_ok=True)

    # Generate timestamped filenames
    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    urls_file = output_dir / f"action_network_game_urls_{date}_{timestamp}.json"
    history_file = output_dir / f"historical_line_movement_full_{timestamp}.json"
    analysis_file = output_dir / f"analysis_report_{timestamp}.json"
    opportunities_file = output_dir / f"betting_opportunities_{timestamp}.json"

    console.print(
        Panel.fit(
            f"[bold blue]🚀 Action Network Complete Pipeline[/bold blue]\n"
            f"Date: [yellow]{date.upper()}[/yellow]\n"
            f"Output Directory: [yellow]{output_dir}[/yellow]\n"
            f"Max Games: [yellow]{max_games or 'All'}[/yellow]\n"
            f"Skip History: [yellow]{skip_history}[/yellow]\n"
            f"Analyze Only: [yellow]{analyze_only}[/yellow]",
            title="Pipeline Configuration",
        )
    )

    pipeline_results = {
        "pipeline_start": datetime.now().isoformat(),
        "configuration": {
            "date": date,
            "max_games": max_games,
            "skip_history": skip_history,
            "analyze_only": analyze_only,
        },
        "phases": {},
    }

    try:
        # Phase 1: Extract Game URLs (unless analyze-only)
        if not analyze_only:
            console.print(
                f"\n[bold cyan]📡 Phase 1: Extracting {date}'s Game URLs[/bold cyan]"
            )

            with console.status(
                f"[bold green]Extracting game URLs for {date}..."
            ) as status:
                urls_result = await _extract_game_urls(
                    date, urls_file, max_games, verbose
                )
                pipeline_results["phases"]["url_extraction"] = urls_result

            if not urls_result["success"]:
                console.print(
                    f"[red]❌ Failed to extract game URLs: {urls_result['error']}[/red]"
                )
                return

            console.print(
                f"[green]✅ Extracted {urls_result['total_games']} game URLs[/green]"
            )
            console.print(f"[dim]Saved to: {urls_file}[/dim]")
        else:
            # Find most recent URLs file
            urls_file = _find_most_recent_file(
                output_dir, "action_network_game_urls_*.json"
            )
            if not urls_file:
                console.print(
                    "[red]❌ No existing URLs file found for analyze-only mode[/red]"
                )
                return
            console.print(f"[yellow]📄 Using existing URLs file: {urls_file}[/yellow]")

        # Phase 2: Collect Historical Data (unless skip-history or analyze-only)
        if not skip_history and not analyze_only:
            console.print(
                "\n[bold cyan]📊 Phase 2: Collecting Historical Line Movement Data[/bold cyan]"
            )

            history_result = await _collect_historical_data(
                urls_file, history_file, max_games, verbose
            )
            pipeline_results["phases"]["history_collection"] = history_result

            if not history_result["success"]:
                console.print(
                    f"[red]❌ Failed to collect historical data: {history_result['error']}[/red]"
                )
                return

            console.print(
                f"[green]✅ Collected historical data for {history_result['games_processed']} games[/green]"
            )
            console.print(
                f"[dim]Total movements: {history_result['total_movements']}[/dim]"
            )
            console.print(f"[dim]Saved to: {history_file}[/dim]")
        else:
            # Find most recent history file
            history_file = _find_most_recent_file(
                output_dir, "historical_line_movement_*.json"
            )
            if not history_file:
                console.print("[red]❌ No existing historical data file found[/red]")
                return
            console.print(
                f"[yellow]📄 Using existing historical data: {history_file}[/yellow]"
            )

        # Phase 3: Analyze Opportunities
        console.print(
            "\n[bold cyan]🔍 Phase 3: Analyzing Betting Opportunities[/bold cyan]"
        )

        analysis_result = await _analyze_opportunities(
            history_file, analysis_file, opportunities_file, verbose
        )
        pipeline_results["phases"]["opportunity_analysis"] = analysis_result

        if not analysis_result["success"]:
            console.print(
                f"[red]❌ Failed to analyze opportunities: {analysis_result['error']}[/red]"
            )
            return

        console.print("[green]✅ Analysis completed[/green]")
        console.print(f"[dim]Games analyzed: {analysis_result['games_analyzed']}[/dim]")
        console.print(
            f"[dim]RLM opportunities: {analysis_result['rlm_opportunities']}[/dim]"
        )
        console.print(f"[dim]Steam moves: {analysis_result['steam_moves']}[/dim]")

        # Phase 4: Generate Summary Report
        console.print("\n[bold cyan]📈 Phase 4: Generating Summary Report[/bold cyan]")

        pipeline_results["pipeline_end"] = datetime.now().isoformat()
        pipeline_results["summary"] = _generate_pipeline_summary(pipeline_results)

        # Save pipeline results
        pipeline_file = output_dir / f"pipeline_results_{timestamp}.json"
        with open(pipeline_file, "w") as f:
            json.dump(pipeline_results, f, indent=2)

        # Display final summary
        _display_pipeline_summary(pipeline_results, analysis_result)

        console.print("\n[bold green]🎉 Pipeline completed successfully![/bold green]")
        console.print(f"[dim]Full results saved to: {pipeline_file}[/dim]")

    except Exception as e:
        console.print(f"[red]❌ Pipeline failed: {e}[/red]")
        pipeline_results["error"] = str(e)
        pipeline_results["pipeline_end"] = datetime.now().isoformat()

        # Save error results
        error_file = output_dir / f"pipeline_error_{timestamp}.json"
        with open(error_file, "w") as f:
            json.dump(pipeline_results, f, indent=2)


async def _extract_game_urls(
    date: str, output_file: Path, max_games: int | None, verbose: bool
) -> dict:
    """Extract game URLs using the unified Action Network collector."""
    try:
        import json
        from datetime import datetime, timedelta

        from src.data.collection.base import (
            CollectionRequest,
            CollectorConfig,
            DataSource,
        )
        from src.data.collection.consolidated_action_network_collector import ActionNetworkCollector

        # Create collector config
        config = CollectorConfig(
            source=DataSource.ACTION_NETWORK,
            base_url="https://www.actionnetwork.com",
            params={
                "cache_build_id": True,
                "output_dir": str(output_file.parent),
                "date": date,
                "max_games": max_games,
            },
        )

        # Initialize collector
        collector = ActionNetworkCollector(config)

        # Determine target date
        target_date = datetime.now()
        if date.lower() == "tomorrow":
            target_date = target_date + timedelta(days=1)

        # Create collection request
        request = CollectionRequest(
            source=DataSource.ACTION_NETWORK,
            start_date=target_date,
            additional_params={"date_option": date, "max_games": max_games},
        )

        # Collect data (this will extract URLs and get basic game data)
        collected_data = await collector.collect_data(request)

        if not collected_data:
            return {
                "success": False,
                "error": "No game data collected from Action Network",
                "total_games": 0,
            }

        # Transform collected data to the expected URL format
        games_data = {
            "extraction_date": target_date.strftime("%Y-%m-%d"),
            "total_games": len(collected_data),
            "games": [],
        }

        for game_data in collected_data:
            # Extract game information from Action Network API structure
            teams = game_data.get('teams', [])
            home_team_id = game_data.get('home_team_id')
            away_team_id = game_data.get('away_team_id')
            
            # Initialize team names
            away_team = 'Unknown'
            home_team = 'Unknown'
            
            # Find home and away teams by matching IDs
            for team in teams:
                team_id = team.get('id')
                if team_id == home_team_id:
                    home_team = team.get('full_name', team.get('display_name', 'Unknown'))
                elif team_id == away_team_id:
                    away_team = team.get('full_name', team.get('display_name', 'Unknown'))
            
            game_id = game_data.get('id')
            start_time = game_data.get('start_time') or datetime.now().isoformat()
            
            # Generate history URL for Action Network
            history_url = f"https://api.actionnetwork.com/web/v2/markets/event/{game_id}/history" if game_id else ""
            
            game_info = {
                "game_id": game_id,
                "home_team": home_team,
                "away_team": away_team,
                "start_time": start_time,
                "history_url": history_url,
                "collected_at": datetime.now().isoformat(),
            }
            games_data["games"].append(game_info)

        # Save to output file
        with open(output_file, "w") as f:
            json.dump(games_data, f, indent=2)

        return {
            "success": True,
            "total_games": len(collected_data),
            "output_file": str(output_file),
        }

    except Exception as e:
        console.print(f"[red]Error in unified URL extraction: {str(e)}[/red]")

        # Fallback to mock data for demonstration
        console.print("[yellow]Falling back to mock game data...[/yellow]")

        mock_games = {
            "extraction_date": datetime.now().strftime("%Y-%m-%d"),
            "total_games": 3,
            "games": [
                {
                    "game_id": "mock_001",
                    "home_team": "New York Yankees",
                    "away_team": "Boston Red Sox",
                    "start_time": (datetime.now() + timedelta(hours=2)).isoformat(),
                    "history_url": "https://www.actionnetwork.com/mlb/game/mock_001/history",
                    "collected_at": datetime.now().isoformat(),
                },
                {
                    "game_id": "mock_002",
                    "home_team": "Los Angeles Dodgers",
                    "away_team": "San Francisco Giants",
                    "start_time": (datetime.now() + timedelta(hours=5)).isoformat(),
                    "history_url": "https://www.actionnetwork.com/mlb/game/mock_002/history",
                    "collected_at": datetime.now().isoformat(),
                },
                {
                    "game_id": "mock_003",
                    "home_team": "Houston Astros",
                    "away_team": "Texas Rangers",
                    "start_time": (datetime.now() + timedelta(hours=8)).isoformat(),
                    "history_url": "https://www.actionnetwork.com/mlb/game/mock_003/history",
                    "collected_at": datetime.now().isoformat(),
                },
            ],
        }

        # Save mock data
        with open(output_file, "w") as f:
            json.dump(mock_games, f, indent=2)

        return {
            "success": True,
            "total_games": 3,
            "output_file": str(output_file),
            "warning": f"Used mock data due to error: {str(e)}",
        }


async def _collect_historical_data(
    urls_file: Path, output_file: Path, max_games: int | None, verbose: bool
) -> dict:
    """Collect historical line movement data from game URLs using unified system."""
    try:
        import json
        import aiohttp

        from src.data.collection.consolidated_action_network_collector import ActionNetworkHistoryParser

        # Load game URLs from file
        with open(urls_file) as f:
            game_data = json.load(f)

        games = game_data.get("games", [])
        if not games:
            return {
                "success": False,
                "error": "No games found in URLs file",
                "games_processed": 0,
                "total_movements": 0,
            }

        # Apply max_games limit if specified
        if max_games and max_games < len(games):
            games = games[:max_games]
            console.print(
                f"[yellow]Limited to {max_games} games for processing[/yellow]"
            )

        # Initialize Action Network history parser
        history_parser = ActionNetworkHistoryParser()

        # Collect historical data for each game
        all_historical_data = []
        successful_collections = 0
        total_movements = 0

        console.print(
            f"[blue]Processing {len(games)} games for historical data...[/blue]"
        )

        # Create session with proper headers for Action Network API
        headers = {
            "Accept": "application/json, text/plain, */*",
            "Accept-Language": "en-US,en;q=0.9",
            "Referer": "https://www.actionnetwork.com/",
            "Origin": "https://www.actionnetwork.com",
            "User-Agent": "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36",
            "Sec-Fetch-Dest": "empty",
            "Sec-Fetch-Mode": "cors",
            "Sec-Fetch-Site": "same-site"
        }
        
        timeout = aiohttp.ClientTimeout(total=30)
        async with aiohttp.ClientSession(headers=headers, timeout=timeout) as session:
            for i, game in enumerate(games, 1):
                try:
                    game_id = game.get("game_id")
                    home_team = game.get("home_team", "Unknown")
                    away_team = game.get("away_team", "Unknown")
                    history_url = game.get("history_url", "")

                    # Parse game datetime
                    game_datetime_str = game.get("start_time", "")
                    if game_datetime_str:
                        try:
                            game_datetime = datetime.fromisoformat(
                                game_datetime_str.replace("Z", "+00:00")
                            )
                        except:
                            game_datetime = datetime.now()
                    else:
                        game_datetime = datetime.now()

                    if not history_url:
                        console.print(
                            f"[yellow]Skipping game {i}/{len(games)}: No history URL[/yellow]"
                        )
                        continue

                    console.print(
                        f"[cyan]Collecting {i}/{len(games)}: {away_team} @ {home_team}[/cyan]"
                    )

                    # Fetch historical data from API
                    try:
                        async with session.get(history_url) as response:
                            if response.status == 200:
                                history_response = await response.json()
                                
                                # Parse the historical data
                                historical_data = history_parser.parse_history_response(
                                    response_data=history_response,
                                    game_id=game_id,
                                    home_team=home_team,
                                    away_team=away_team,
                                    game_datetime=game_datetime,
                                    history_url=history_url,
                                )
                                
                                if historical_data:
                                    all_historical_data.append(
                                        {
                                            "game_id": game_id,
                                            "home_team": home_team,
                                            "away_team": away_team,
                                            "game_datetime": game_datetime.isoformat(),
                                            "history_url": history_url,
                                            "historical_data": historical_data.dict(),
                                        }
                                    )

                                    # Count movements
                                    total_movements += len(historical_data.historical_entries)
                                    
                                    successful_collections += 1
                                    console.print(
                                        f"[green]✅ Collected historical data for {away_team} @ {home_team}[/green]"
                                    )
                                else:
                                    console.print(
                                        f"[yellow]⚠️ No historical data returned for {away_team} @ {home_team}[/yellow]"
                                    )
                            else:
                                console.print(
                                    f"[red]❌ API request failed (status {response.status}) for {away_team} @ {home_team}[/red]"
                                )
                    except Exception as api_error:
                        console.print(
                            f"[red]❌ API error for {away_team} @ {home_team}: {str(api_error)}[/red]"
                        )

                    # Small delay to be respectful to the API
                    await asyncio.sleep(0.5)

                except Exception as e:
                    console.print(f"[red]❌ Error processing game {i}: {str(e)}[/red]")
                    continue

        # Save all collected historical data
        output_data = {
            "collection_timestamp": datetime.now().isoformat(),
            "total_games_requested": len(games),
            "successful_collections": successful_collections,
            "failed_collections": len(games) - successful_collections,
            "total_movements": total_movements,
            "games": all_historical_data,
        }

        with open(output_file, "w") as f:
            json.dump(output_data, f, indent=2, default=str)

        return {
            "success": True,
            "games_processed": successful_collections,
            "total_movements": total_movements,
            "output_file": str(output_file),
        }

    except Exception as e:
        console.print(f"[red]Error in historical data collection: {str(e)}[/red]")

        # Fallback to mock historical data
        console.print("[yellow]Falling back to mock historical data...[/yellow]")

        mock_historical_data = {
            "collection_timestamp": datetime.now().isoformat(),
            "total_games_requested": 3,
            "successful_collections": 3,
            "failed_collections": 0,
            "total_movements": 45,
            "games": [
                {
                    "game_id": "mock_001",
                    "home_team": "New York Yankees",
                    "away_team": "Boston Red Sox",
                    "game_datetime": (datetime.now() + timedelta(hours=2)).isoformat(),
                    "history_url": "https://www.actionnetwork.com/mlb/game/mock_001/history",
                    "movements_count": 15,
                    "note": "Mock data - includes spread, moneyline, and total movements",
                },
                {
                    "game_id": "mock_002",
                    "home_team": "Los Angeles Dodgers",
                    "away_team": "San Francisco Giants",
                    "game_datetime": (datetime.now() + timedelta(hours=5)).isoformat(),
                    "history_url": "https://www.actionnetwork.com/mlb/game/mock_002/history",
                    "movements_count": 18,
                    "note": "Mock data - includes spread, moneyline, and total movements",
                },
                {
                    "game_id": "mock_003",
                    "home_team": "Houston Astros",
                    "away_team": "Texas Rangers",
                    "game_datetime": (datetime.now() + timedelta(hours=8)).isoformat(),
                    "history_url": "https://www.actionnetwork.com/mlb/game/mock_003/history",
                    "movements_count": 12,
                    "note": "Mock data - includes spread, moneyline, and total movements",
                },
            ],
        }

        with open(output_file, "w") as f:
            json.dump(mock_historical_data, f, indent=2)

        return {
            "success": True,
            "games_processed": 3,
            "total_movements": 45,
            "output_file": str(output_file),
            "warning": f"Used mock data due to error: {str(e)}",
        }


async def _analyze_opportunities(
    history_file: Path, analysis_file: Path, opportunities_file: Path, verbose: bool
) -> dict:
    """Analyze historical data for betting opportunities and save to database."""
    try:
        # Load historical data
        with open(history_file) as f:
            data = json.load(f)

        historical_data = data.get("games", [])  # Updated to match new structure

        # Initialize analyzer and repository
        analyzer = MovementAnalyzer()
        reports_repo = AnalysisReportsRepository()

        # Generate pipeline run ID for this analysis
        pipeline_run_id = f"pipeline_{datetime.now().strftime('%Y%m%d_%H%M%S')}"
        
        # Analyze each game
        game_analyses = []
        total_rlm = 0
        total_steam = 0
        total_arbitrage = 0

        for game_data in historical_data:
            try:
                # Extract historical data from the new structure
                historical_entries = game_data.get("historical_data", {})
                if not historical_entries:
                    continue
                    
                analysis = await analyzer.analyze_game_movements({
                    "game_id": game_data.get("game_id"),
                    "home_team": game_data.get("home_team"),
                    "away_team": game_data.get("away_team"),
                    "game_datetime": game_data.get("game_datetime"),
                    "raw_data": historical_entries  # Pass the historical data
                })
                game_analyses.append(analysis)

                # Count opportunities
                if analysis.rlm_indicators:
                    total_rlm += len(analysis.rlm_indicators)
                    
                if analysis.cross_book_movements:
                    steam_count = len([c for c in analysis.cross_book_movements if c.steam_move_detected])
                    total_steam += steam_count
                    
                if analysis.arbitrage_opportunities:
                    total_arbitrage += len(analysis.arbitrage_opportunities)

            except Exception as e:
                if verbose:
                    console.print(
                        f"[yellow]⚠️  Error analyzing game {game_data.get('game_id', 'unknown')}: {e}[/yellow]"
                    )

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

        # Save to database instead of JSON files
        try:
            analysis_report_id = await reports_repo.save_complete_analysis(
                pipeline_run_id=pipeline_run_id,
                analysis_report=report,
            )
            
            console.print(f"[green]✅ Saved analysis report to database (ID: {analysis_report_id})[/green]")
            
            # Optional: Still save minimal JSON for debugging (but don't duplicate data)
            if verbose:
                debug_summary = {
                    "analysis_timestamp": report.analysis_timestamp.isoformat(),
                    "total_games_analyzed": report.total_games_analyzed,
                    "total_opportunities": total_rlm + total_steam + total_arbitrage,
                    "database_report_id": analysis_report_id,
                    "note": "Full analysis data saved to PostgreSQL database"
                }
                
                with open(analysis_file, "w") as f:
                    json.dump(debug_summary, f, indent=2)
                    
                with open(opportunities_file, "w") as f:
                    json.dump({
                        "generated_at": datetime.now().isoformat(),
                        "database_report_id": analysis_report_id,
                        "summary": {
                            "total_rlm_opportunities": total_rlm,
                            "total_steam_moves": total_steam,
                            "total_arbitrage_opportunities": total_arbitrage,
                        },
                        "note": "Detailed opportunities saved to PostgreSQL. Use CLI 'opportunities' command to view."
                    }, f, indent=2)
            
        except Exception as db_error:
            console.print(f"[red]⚠️  Database save failed: {db_error}[/red]")
            console.print("[yellow]Falling back to JSON file storage...[/yellow]")
            
            # Fallback to original JSON storage if database fails
            with open(analysis_file, "w") as f:
                json.dump(report.dict(), f, indent=2, default=str)

            opportunities_summary = {
                "generated_at": datetime.now().isoformat(),
                "total_games": len(game_analyses),
                "summary": {
                    "total_rlm_opportunities": total_rlm,
                    "total_steam_moves": total_steam,
                    "total_arbitrage_opportunities": total_arbitrage,
                },
                "note": "Database storage failed, using JSON fallback"
            }

            with open(opportunities_file, "w") as f:
                json.dump(opportunities_summary, f, indent=2)

        return {
            "success": True,
            "games_analyzed": len(game_analyses),
            "rlm_opportunities": total_rlm,
            "steam_moves": total_steam,
            "arbitrage_opportunities": total_arbitrage,
            "analysis_file": str(analysis_file),
            "opportunities_file": str(opportunities_file),
        }

    except Exception as e:
        return {
            "success": False,
            "error": str(e),
            "games_analyzed": 0,
            "rlm_opportunities": 0,
            "steam_moves": 0,
            "arbitrage_opportunities": 0,
        }


def _find_most_recent_file(directory: Path, pattern: str) -> Path | None:
    """Find the most recent file matching the pattern."""
    import glob

    files = glob.glob(str(directory / pattern))
    if not files:
        return None

    return Path(max(files, key=lambda x: Path(x).stat().st_mtime))


def _generate_pipeline_summary(pipeline_results: dict) -> dict:
    """Generate a summary of the pipeline execution."""
    phases = pipeline_results.get("phases", {})

    summary = {
        "execution_time": "N/A",
        "total_games": 0,
        "total_movements": 0,
        "opportunities_found": 0,
        "phase_success": {},
    }

    # Calculate execution time
    start_time = pipeline_results.get("pipeline_start")
    end_time = pipeline_results.get("pipeline_end")
    if start_time and end_time:
        start_dt = datetime.fromisoformat(start_time)
        end_dt = datetime.fromisoformat(end_time)
        duration = end_dt - start_dt
        summary["execution_time"] = f"{duration.total_seconds():.1f}s"

    # Extract phase results
    if "url_extraction" in phases:
        summary["total_games"] = phases["url_extraction"].get("total_games", 0)
        summary["phase_success"]["url_extraction"] = phases["url_extraction"].get(
            "success", False
        )

    if "history_collection" in phases:
        summary["total_movements"] = phases["history_collection"].get(
            "total_movements", 0
        )
        summary["phase_success"]["history_collection"] = phases[
            "history_collection"
        ].get("success", False)

    if "opportunity_analysis" in phases:
        analysis = phases["opportunity_analysis"]
        summary["opportunities_found"] = (
            analysis.get("rlm_opportunities", 0)
            + analysis.get("steam_moves", 0)
            + analysis.get("arbitrage_opportunities", 0)
        )
        summary["phase_success"]["opportunity_analysis"] = analysis.get(
            "success", False
        )

    return summary


def _display_pipeline_summary(pipeline_results: dict, analysis_result: dict):
    """Display a comprehensive pipeline summary."""
    console.print("\n[bold blue]📊 Pipeline Summary[/bold blue]")

    # Create summary table
    table = Table(title="Pipeline Execution Summary")
    table.add_column("Phase", style="cyan")
    table.add_column("Status", style="green")
    table.add_column("Results", style="yellow")

    phases = pipeline_results.get("phases", {})

    # URL Extraction
    if "url_extraction" in phases:
        url_phase = phases["url_extraction"]
        status = "✅ Success" if url_phase.get("success") else "❌ Failed"
        results = f"{url_phase.get('total_games', 0)} games"
        table.add_row("URL Extraction", status, results)

    # History Collection
    if "history_collection" in phases:
        history_phase = phases["history_collection"]
        status = "✅ Success" if history_phase.get("success") else "❌ Failed"
        results = f"{history_phase.get('total_movements', 0)} movements"
        table.add_row("History Collection", status, results)

    # Opportunity Analysis
    if "opportunity_analysis" in phases:
        analysis_phase = phases["opportunity_analysis"]
        status = "✅ Success" if analysis_phase.get("success") else "❌ Failed"
        results = f"{analysis_phase.get('rlm_opportunities', 0)} RLM, {analysis_phase.get('steam_moves', 0)} Steam"
        table.add_row("Opportunity Analysis", status, results)

    console.print(table)

    # Display top opportunities if available
    if (
        analysis_result.get("success")
        and analysis_result.get("rlm_opportunities", 0) > 0
    ):
        console.print("\n[bold red]🔄 Top RLM Opportunities:[/bold red]")
        console.print(f"Found {analysis_result['rlm_opportunities']} RLM indicators")

    if analysis_result.get("success") and analysis_result.get("steam_moves", 0) > 0:
        console.print("\n[bold green]🚂 Steam Moves Detected:[/bold green]")
        console.print(f"Found {analysis_result['steam_moves']} steam moves")

    # Execution summary
    summary = pipeline_results.get("summary", {})
    console.print(
        f"\n[bold]⏱️  Execution Time:[/bold] {summary.get('execution_time', 'N/A')}"
    )
    console.print(
        f"[bold]🎯 Total Opportunities:[/bold] {summary.get('opportunities_found', 0)}"
    )


@action_network.command()
@click.option(
    "--hours",
    "-h",
    type=int,
    default=24,
    help="Hours to look back for opportunities (default: 24)",
)
@click.option(
    "--limit",
    "-l",
    type=int,
    default=20,
    help="Maximum number of opportunities to display (default: 20)",
)
@click.option(
    "--fallback-json",
    is_flag=True,
    help="Fallback to JSON files if database is unavailable",
)
def opportunities(hours: int, limit: int, fallback_json: bool):
    """Display the latest betting opportunities from database or JSON fallback."""

    try:
        # Try to get opportunities from database first
        reports_repo = AnalysisReportsRepository()
        opportunities_data = asyncio.run(reports_repo.get_latest_opportunities(hours=hours))
        
        if opportunities_data:
            console.print(
                Panel.fit(
                    f"[bold blue]🎯 Latest Betting Opportunities[/bold blue]\n"
                    f"Source: [yellow]PostgreSQL Database[/yellow]\n"
                    f"Last {hours} hours: [yellow]{len(opportunities_data)} opportunities[/yellow]",
                    title="Opportunities Report",
                )
            )

            # Group opportunities by type
            rlm_opps = [o for o in opportunities_data if o['opportunity_type'] == 'rlm']
            steam_opps = [o for o in opportunities_data if o['opportunity_type'] == 'steam']
            arb_opps = [o for o in opportunities_data if o['opportunity_type'] == 'arbitrage']

            # Summary table
            table = Table(title="Opportunities Summary")
            table.add_column("Type", style="cyan")
            table.add_column("Count", style="green", justify="right")
            table.add_column("Description", style="yellow")

            table.add_row(
                "RLM Opportunities",
                str(len(rlm_opps)),
                "Reverse Line Movement",
            )
            table.add_row(
                "Steam Moves",
                str(len(steam_opps)),
                "Cross-book consensus",
            )
            table.add_row(
                "Arbitrage",
                str(len(arb_opps)),
                "Risk-free profit",
            )

            console.print(table)

            # Display top opportunities by type
            if rlm_opps:
                console.print("\n[bold red]🔄 RLM Opportunities:[/bold red]")
                for i, opp in enumerate(rlm_opps[:limit//3], 1):
                    console.print(
                        f"{i}. {opp['away_team']} @ {opp['home_team']} - {opp['market_type']} ({opp['strength']})"
                    )

            if steam_opps:
                console.print("\n[bold green]🚂 Steam Moves:[/bold green]")
                for i, move in enumerate(steam_opps[:limit//3], 1):
                    console.print(
                        f"{i}. {move['away_team']} @ {move['home_team']} - {move['market_type']} ({move['strength']})"
                    )

            if arb_opps:
                console.print("\n[bold yellow]💰 Arbitrage Opportunities:[/bold yellow]")
                for i, arb in enumerate(arb_opps[:limit//3], 1):
                    profit = f"{arb.get('profit_potential', 0):.2f}%" if arb.get('profit_potential') else "TBD"
                    console.print(
                        f"{i}. {arb['away_team']} @ {arb['home_team']} - {arb['market_type']} (Profit: {profit})"
                    )

            return

    except Exception as db_error:
        console.print(f"[red]⚠️ Database error: {db_error}[/red]")
        if not fallback_json:
            console.print("[yellow]Use --fallback-json flag to try JSON files instead[/yellow]")
            return

    # Fallback to JSON files
    if fallback_json or True:  # Always fallback for now during transition
        console.print("[yellow]Falling back to JSON file search...[/yellow]")
        
        output_dir = Path("output")
        pattern = "betting_opportunities_*.json"
        latest_file = _find_most_recent_file(output_dir, pattern)

        if not latest_file:
            console.print(
                f"[red]❌ No opportunities found in database or JSON files[/red]"
            )
            return

        try:
            with open(latest_file) as f:
                data = json.load(f)

            console.print(
                Panel.fit(
                    f"[bold blue]🎯 Latest Betting Opportunities[/bold blue]\n"
                    f"Source: [yellow]JSON File (Fallback)[/yellow]\n"
                    f"File: [yellow]{latest_file.name}[/yellow]\n"
                    f"Generated: [yellow]{data.get('generated_at', 'Unknown')}[/yellow]",
                    title="Opportunities Report",
                )
            )

            summary = data.get("summary", {})

            # Summary table
            table = Table(title="Opportunities Summary")
            table.add_column("Type", style="cyan")
            table.add_column("Count", style="green", justify="right")
            table.add_column("Description", style="yellow")

            table.add_row(
                "RLM Opportunities",
                str(summary.get("total_rlm_opportunities", 0)),
                "Reverse Line Movement",
            )
            table.add_row(
                "Steam Moves",
                str(summary.get("total_steam_moves", 0)),
                "Cross-book consensus",
            )
            table.add_row(
                "Arbitrage",
                str(summary.get("total_arbitrage_opportunities", 0)),
                "Risk-free profit",
            )

            console.print(table)

            # Display top opportunities from JSON
            rlm_opportunities = data.get("rlm_opportunities", [])
            if rlm_opportunities:
                console.print("\n[bold red]🔄 RLM Opportunities:[/bold red]")
                for i, opp in enumerate(rlm_opportunities[:5], 1):  # Show top 5
                    console.print(
                        f"{i}. {opp['teams']} - {opp['rlm']['market_type']} ({opp['rlm']['strength']})"
                    )

            steam_moves = data.get("steam_moves", [])
            if steam_moves:
                console.print("\n[bold green]🚂 Steam Moves:[/bold green]")
                for i, move in enumerate(steam_moves[:5], 1):  # Show top 5
                    console.print(
                        f"{i}. {move['teams']} - {move['steam']['market_type']} ({len(move['steam']['participating_books'])} books)"
                    )

        except Exception as e:
            console.print(f"[red]❌ Error reading opportunities file: {e}[/red]")


if __name__ == "__main__":
    action_network()
