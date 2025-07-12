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
from datetime import datetime
from pathlib import Path
from typing import Dict, List, Optional

import click
from rich.console import Console
from rich.panel import Panel
from rich.progress import Progress, SpinnerColumn, TextColumn, BarColumn, TaskProgressColumn
from rich.table import Table
from rich.text import Text

from src.analysis.processors.movement_analyzer import MovementAnalyzer
from src.data.models.unified.movement_analysis import MovementAnalysisReport

console = Console()


@click.group()
def action_network():
    """Action Network data collection and analysis pipeline."""
    pass


@action_network.command()
@click.option('--date', '-d', 
              type=click.Choice(['today', 'tomorrow']), 
              default='today',
              help='Date to collect data for')
@click.option('--output-dir', '-o',
              type=click.Path(path_type=Path),
              default=Path('output'),
              help='Directory to save output files')
@click.option('--max-games', '-m',
              type=int,
              help='Maximum number of games to process (for testing)')
@click.option('--skip-history', '-s',
              is_flag=True,
              help='Skip historical data collection (URLs only)')
@click.option('--analyze-only', '-a',
              is_flag=True,
              help='Skip collection, analyze existing data only')
@click.option('--verbose', '-v',
              is_flag=True,
              help='Enable verbose logging')
async def pipeline(
    date: str,
    output_dir: Path,
    max_games: Optional[int],
    skip_history: bool,
    analyze_only: bool,
    verbose: bool
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
    
    # Ensure output directory exists
    output_dir.mkdir(exist_ok=True)
    
    # Generate timestamped filenames
    timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
    urls_file = output_dir / f"action_network_game_urls_{date}_{timestamp}.json"
    history_file = output_dir / f"historical_line_movement_full_{timestamp}.json"
    analysis_file = output_dir / f"analysis_report_{timestamp}.json"
    opportunities_file = output_dir / f"betting_opportunities_{timestamp}.json"
    
    console.print(Panel.fit(
        f"[bold blue]🚀 Action Network Complete Pipeline[/bold blue]\n"
        f"Date: [yellow]{date.upper()}[/yellow]\n"
        f"Output Directory: [yellow]{output_dir}[/yellow]\n"
        f"Max Games: [yellow]{max_games or 'All'}[/yellow]\n"
        f"Skip History: [yellow]{skip_history}[/yellow]\n"
        f"Analyze Only: [yellow]{analyze_only}[/yellow]",
        title="Pipeline Configuration"
    ))
    
    pipeline_results = {
        'pipeline_start': datetime.now().isoformat(),
        'configuration': {
            'date': date,
            'max_games': max_games,
            'skip_history': skip_history,
            'analyze_only': analyze_only
        },
        'phases': {}
    }
    
    try:
        # Phase 1: Extract Game URLs (unless analyze-only)
        if not analyze_only:
            console.print(f"\n[bold cyan]📡 Phase 1: Extracting {date}'s Game URLs[/bold cyan]")
            
            with console.status(f"[bold green]Extracting game URLs for {date}...") as status:
                urls_result = await _extract_game_urls(date, urls_file, max_games, verbose)
                pipeline_results['phases']['url_extraction'] = urls_result
            
            if not urls_result['success']:
                console.print(f"[red]❌ Failed to extract game URLs: {urls_result['error']}[/red]")
                return
            
            console.print(f"[green]✅ Extracted {urls_result['total_games']} game URLs[/green]")
            console.print(f"[dim]Saved to: {urls_file}[/dim]")
        else:
            # Find most recent URLs file
            urls_file = _find_most_recent_file(output_dir, "action_network_game_urls_*.json")
            if not urls_file:
                console.print("[red]❌ No existing URLs file found for analyze-only mode[/red]")
                return
            console.print(f"[yellow]📄 Using existing URLs file: {urls_file}[/yellow]")
        
        # Phase 2: Collect Historical Data (unless skip-history or analyze-only)
        if not skip_history and not analyze_only:
            console.print(f"\n[bold cyan]📊 Phase 2: Collecting Historical Line Movement Data[/bold cyan]")
            
            history_result = await _collect_historical_data(urls_file, history_file, max_games, verbose)
            pipeline_results['phases']['history_collection'] = history_result
            
            if not history_result['success']:
                console.print(f"[red]❌ Failed to collect historical data: {history_result['error']}[/red]")
                return
            
            console.print(f"[green]✅ Collected historical data for {history_result['games_processed']} games[/green]")
            console.print(f"[dim]Total movements: {history_result['total_movements']}[/dim]")
            console.print(f"[dim]Saved to: {history_file}[/dim]")
        else:
            # Find most recent history file
            history_file = _find_most_recent_file(output_dir, "historical_line_movement_*.json")
            if not history_file:
                console.print("[red]❌ No existing historical data file found[/red]")
                return
            console.print(f"[yellow]📄 Using existing historical data: {history_file}[/yellow]")
        
        # Phase 3: Analyze Opportunities
        console.print(f"\n[bold cyan]🔍 Phase 3: Analyzing Betting Opportunities[/bold cyan]")
        
        analysis_result = await _analyze_opportunities(history_file, analysis_file, opportunities_file, verbose)
        pipeline_results['phases']['opportunity_analysis'] = analysis_result
        
        if not analysis_result['success']:
            console.print(f"[red]❌ Failed to analyze opportunities: {analysis_result['error']}[/red]")
            return
        
        console.print(f"[green]✅ Analysis completed[/green]")
        console.print(f"[dim]Games analyzed: {analysis_result['games_analyzed']}[/dim]")
        console.print(f"[dim]RLM opportunities: {analysis_result['rlm_opportunities']}[/dim]")
        console.print(f"[dim]Steam moves: {analysis_result['steam_moves']}[/dim]")
        
        # Phase 4: Generate Summary Report
        console.print(f"\n[bold cyan]📈 Phase 4: Generating Summary Report[/bold cyan]")
        
        pipeline_results['pipeline_end'] = datetime.now().isoformat()
        pipeline_results['summary'] = _generate_pipeline_summary(pipeline_results)
        
        # Save pipeline results
        pipeline_file = output_dir / f"pipeline_results_{timestamp}.json"
        with open(pipeline_file, 'w') as f:
            json.dump(pipeline_results, f, indent=2)
        
        # Display final summary
        _display_pipeline_summary(pipeline_results, analysis_result)
        
        console.print(f"\n[bold green]🎉 Pipeline completed successfully![/bold green]")
        console.print(f"[dim]Full results saved to: {pipeline_file}[/dim]")
        
    except Exception as e:
        console.print(f"[red]❌ Pipeline failed: {e}[/red]")
        pipeline_results['error'] = str(e)
        pipeline_results['pipeline_end'] = datetime.now().isoformat()
        
        # Save error results
        error_file = output_dir / f"pipeline_error_{timestamp}.json"
        with open(error_file, 'w') as f:
            json.dump(pipeline_results, f, indent=2)


async def _extract_game_urls(date: str, output_file: Path, max_games: Optional[int], verbose: bool) -> Dict:
    """Extract game URLs using the existing Action Network extractor."""
    try:
        import subprocess
        import sys
        
        # Run the existing URL extractor
        cmd = [
            'uv', 'run', 'python', '-m', 'action.extract_todays_game_urls',
            '--date', date
        ]
        
        if not verbose:
            cmd.append('--no-test')
        
        result = subprocess.run(cmd, capture_output=True, text=True, cwd=Path.cwd())
        
        if result.returncode != 0:
            return {
                'success': False,
                'error': f"URL extraction failed: {result.stderr}",
                'total_games': 0
            }
        
        # Find the generated file (it has a different timestamp)
        import glob
        pattern = f"output/action_network_game_urls_{date}_*.json"
        files = glob.glob(pattern)
        
        if not files:
            return {
                'success': False,
                'error': "No URLs file generated",
                'total_games': 0
            }
        
        # Use the most recent file
        latest_file = max(files, key=lambda x: Path(x).stat().st_mtime)
        
        # Load the data to get game count
        with open(latest_file, 'r') as f:
            data = json.load(f)
        
        total_games = len(data.get('games', []))
        
        # Copy to our desired filename
        import shutil
        shutil.copy2(latest_file, output_file)
        
        return {
            'success': True,
            'total_games': total_games,
            'source_file': latest_file,
            'output_file': str(output_file)
        }
        
    except Exception as e:
        return {
            'success': False,
            'error': str(e),
            'total_games': 0
        }


async def _collect_historical_data(urls_file: Path, output_file: Path, max_games: Optional[int], verbose: bool) -> Dict:
    """Collect historical line movement data from game URLs."""
    try:
        # Use the existing unified data service
        from src.services.data.unified_data_service import get_unified_data_service
        
        service = get_unified_data_service()
        
        # Extract histories from the URLs file
        result = await service.extract_histories_from_json_file(
            json_file_path=urls_file,
            max_games=max_games
        )
        
        if not result:
            return {
                'success': False,
                'error': "No historical data collected",
                'games_processed': 0,
                'total_movements': 0
            }
        
        # Save the collected data
        await service.save_historical_data_to_json(result, output_file)
        
        # Calculate statistics
        total_movements = sum(len(game.get('historical_entries', [])) for game in result)
        
        return {
            'success': True,
            'games_processed': len(result),
            'total_movements': total_movements,
            'output_file': str(output_file)
        }
        
    except Exception as e:
        return {
            'success': False,
            'error': str(e),
            'games_processed': 0,
            'total_movements': 0
        }


async def _analyze_opportunities(history_file: Path, analysis_file: Path, opportunities_file: Path, verbose: bool) -> Dict:
    """Analyze historical data for betting opportunities."""
    try:
        # Load historical data
        with open(history_file, 'r') as f:
            data = json.load(f)
        
        historical_data = data.get('historical_data', [])
        
        # Initialize analyzer
        analyzer = MovementAnalyzer()
        
        # Analyze each game
        game_analyses = []
        rlm_opportunities = []
        steam_moves = []
        arbitrage_opportunities = []
        
        for game_data in historical_data:
            try:
                analysis = await analyzer.analyze_game_movements(game_data)
                game_analyses.append(analysis)
                
                # Collect opportunities
                if analysis.rlm_indicators:
                    for rlm in analysis.rlm_indicators:
                        rlm_opportunities.append({
                            'game_id': analysis.game_id,
                            'teams': f"{analysis.away_team} @ {analysis.home_team}",
                            'rlm': rlm.dict()
                        })
                
                if analysis.cross_book_movements:
                    for steam in analysis.cross_book_movements:
                        if steam.steam_move_detected:
                            steam_moves.append({
                                'game_id': analysis.game_id,
                                'teams': f"{analysis.away_team} @ {analysis.home_team}",
                                'steam': steam.dict()
                            })
                
                if analysis.arbitrage_opportunities:
                    for arb in analysis.arbitrage_opportunities:
                        arbitrage_opportunities.append({
                            'game_id': analysis.game_id,
                            'teams': f"{analysis.away_team} @ {analysis.home_team}",
                            'arbitrage': arb.dict()
                        })
                
            except Exception as e:
                if verbose:
                    console.print(f"[yellow]⚠️  Error analyzing game {game_data.get('game_id', 'unknown')}: {e}[/yellow]")
        
        # Create comprehensive report
        report = MovementAnalysisReport(
            analysis_timestamp=datetime.now(),
            total_games_analyzed=len(game_analyses),
            games_with_rlm=len([g for g in game_analyses if g.rlm_indicators]),
            games_with_steam_moves=len([g for g in game_analyses if any(c.steam_move_detected for c in g.cross_book_movements)]),
            games_with_arbitrage=len([g for g in game_analyses if g.arbitrage_opportunities]),
            game_analyses=game_analyses
        )
        
        # Save detailed analysis
        with open(analysis_file, 'w') as f:
            json.dump(report.dict(), f, indent=2, default=str)
        
        # Save opportunities summary
        opportunities_summary = {
            'generated_at': datetime.now().isoformat(),
            'total_games': len(game_analyses),
            'rlm_opportunities': rlm_opportunities,
            'steam_moves': steam_moves,
            'arbitrage_opportunities': arbitrage_opportunities,
            'summary': {
                'total_rlm_opportunities': len(rlm_opportunities),
                'total_steam_moves': len(steam_moves),
                'total_arbitrage_opportunities': len(arbitrage_opportunities),
                'games_with_opportunities': len(set(
                    [opp['game_id'] for opp in rlm_opportunities] +
                    [opp['game_id'] for opp in steam_moves] +
                    [opp['game_id'] for opp in arbitrage_opportunities]
                ))
            }
        }
        
        with open(opportunities_file, 'w') as f:
            json.dump(opportunities_summary, f, indent=2)
        
        return {
            'success': True,
            'games_analyzed': len(game_analyses),
            'rlm_opportunities': len(rlm_opportunities),
            'steam_moves': len(steam_moves),
            'arbitrage_opportunities': len(arbitrage_opportunities),
            'analysis_file': str(analysis_file),
            'opportunities_file': str(opportunities_file)
        }
        
    except Exception as e:
        return {
            'success': False,
            'error': str(e),
            'games_analyzed': 0,
            'rlm_opportunities': 0,
            'steam_moves': 0,
            'arbitrage_opportunities': 0
        }


def _find_most_recent_file(directory: Path, pattern: str) -> Optional[Path]:
    """Find the most recent file matching the pattern."""
    import glob
    
    files = glob.glob(str(directory / pattern))
    if not files:
        return None
    
    return Path(max(files, key=lambda x: Path(x).stat().st_mtime))


def _generate_pipeline_summary(pipeline_results: Dict) -> Dict:
    """Generate a summary of the pipeline execution."""
    phases = pipeline_results.get('phases', {})
    
    summary = {
        'execution_time': 'N/A',
        'total_games': 0,
        'total_movements': 0,
        'opportunities_found': 0,
        'phase_success': {}
    }
    
    # Calculate execution time
    start_time = pipeline_results.get('pipeline_start')
    end_time = pipeline_results.get('pipeline_end')
    if start_time and end_time:
        start_dt = datetime.fromisoformat(start_time)
        end_dt = datetime.fromisoformat(end_time)
        duration = end_dt - start_dt
        summary['execution_time'] = f"{duration.total_seconds():.1f}s"
    
    # Extract phase results
    if 'url_extraction' in phases:
        summary['total_games'] = phases['url_extraction'].get('total_games', 0)
        summary['phase_success']['url_extraction'] = phases['url_extraction'].get('success', False)
    
    if 'history_collection' in phases:
        summary['total_movements'] = phases['history_collection'].get('total_movements', 0)
        summary['phase_success']['history_collection'] = phases['history_collection'].get('success', False)
    
    if 'opportunity_analysis' in phases:
        analysis = phases['opportunity_analysis']
        summary['opportunities_found'] = (
            analysis.get('rlm_opportunities', 0) +
            analysis.get('steam_moves', 0) +
            analysis.get('arbitrage_opportunities', 0)
        )
        summary['phase_success']['opportunity_analysis'] = analysis.get('success', False)
    
    return summary


def _display_pipeline_summary(pipeline_results: Dict, analysis_result: Dict):
    """Display a comprehensive pipeline summary."""
    console.print(f"\n[bold blue]📊 Pipeline Summary[/bold blue]")
    
    # Create summary table
    table = Table(title="Pipeline Execution Summary")
    table.add_column("Phase", style="cyan")
    table.add_column("Status", style="green")
    table.add_column("Results", style="yellow")
    
    phases = pipeline_results.get('phases', {})
    
    # URL Extraction
    if 'url_extraction' in phases:
        url_phase = phases['url_extraction']
        status = "✅ Success" if url_phase.get('success') else "❌ Failed"
        results = f"{url_phase.get('total_games', 0)} games"
        table.add_row("URL Extraction", status, results)
    
    # History Collection
    if 'history_collection' in phases:
        history_phase = phases['history_collection']
        status = "✅ Success" if history_phase.get('success') else "❌ Failed"
        results = f"{history_phase.get('total_movements', 0)} movements"
        table.add_row("History Collection", status, results)
    
    # Opportunity Analysis
    if 'opportunity_analysis' in phases:
        analysis_phase = phases['opportunity_analysis']
        status = "✅ Success" if analysis_phase.get('success') else "❌ Failed"
        results = f"{analysis_phase.get('rlm_opportunities', 0)} RLM, {analysis_phase.get('steam_moves', 0)} Steam"
        table.add_row("Opportunity Analysis", status, results)
    
    console.print(table)
    
    # Display top opportunities if available
    if analysis_result.get('success') and analysis_result.get('rlm_opportunities', 0) > 0:
        console.print(f"\n[bold red]🔄 Top RLM Opportunities:[/bold red]")
        console.print(f"Found {analysis_result['rlm_opportunities']} RLM indicators")
    
    if analysis_result.get('success') and analysis_result.get('steam_moves', 0) > 0:
        console.print(f"\n[bold green]🚂 Steam Moves Detected:[/bold green]")
        console.print(f"Found {analysis_result['steam_moves']} steam moves")
    
    # Execution summary
    summary = pipeline_results.get('summary', {})
    console.print(f"\n[bold]⏱️  Execution Time:[/bold] {summary.get('execution_time', 'N/A')}")
    console.print(f"[bold]🎯 Total Opportunities:[/bold] {summary.get('opportunities_found', 0)}")


@action_network.command()
@click.option('--output-dir', '-o',
              type=click.Path(path_type=Path),
              default=Path('output'),
              help='Directory to search for files')
@click.option('--pattern', '-p',
              default='betting_opportunities_*.json',
              help='File pattern to search for')
def opportunities(output_dir: Path, pattern: str):
    """Display the latest betting opportunities from pipeline results."""
    
    latest_file = _find_most_recent_file(output_dir, pattern)
    
    if not latest_file:
        console.print(f"[red]❌ No opportunities file found matching pattern: {pattern}[/red]")
        return
    
    try:
        with open(latest_file, 'r') as f:
            data = json.load(f)
        
        console.print(Panel.fit(
            f"[bold blue]🎯 Latest Betting Opportunities[/bold blue]\n"
            f"File: [yellow]{latest_file.name}[/yellow]\n"
            f"Generated: [yellow]{data.get('generated_at', 'Unknown')}[/yellow]",
            title="Opportunities Report"
        ))
        
        summary = data.get('summary', {})
        
        # Summary table
        table = Table(title="Opportunities Summary")
        table.add_column("Type", style="cyan")
        table.add_column("Count", style="green", justify="right")
        table.add_column("Description", style="yellow")
        
        table.add_row("RLM Opportunities", str(summary.get('total_rlm_opportunities', 0)), "Reverse Line Movement")
        table.add_row("Steam Moves", str(summary.get('total_steam_moves', 0)), "Cross-book consensus")
        table.add_row("Arbitrage", str(summary.get('total_arbitrage_opportunities', 0)), "Risk-free profit")
        table.add_row("Games with Opportunities", str(summary.get('games_with_opportunities', 0)), "Unique games")
        
        console.print(table)
        
        # Display top opportunities
        rlm_opportunities = data.get('rlm_opportunities', [])
        if rlm_opportunities:
            console.print(f"\n[bold red]🔄 RLM Opportunities:[/bold red]")
            for i, opp in enumerate(rlm_opportunities[:5], 1):  # Show top 5
                console.print(f"{i}. {opp['teams']} - {opp['rlm']['market_type']} ({opp['rlm']['strength']})")
        
        steam_moves = data.get('steam_moves', [])
        if steam_moves:
            console.print(f"\n[bold green]🚂 Steam Moves:[/bold green]")
            for i, move in enumerate(steam_moves[:5], 1):  # Show top 5
                console.print(f"{i}. {move['teams']} - {move['steam']['market_type']} ({len(move['steam']['participating_books'])} books)")
        
    except Exception as e:
        console.print(f"[red]❌ Error reading opportunities file: {e}[/red]")


if __name__ == '__main__':
    action_network() 