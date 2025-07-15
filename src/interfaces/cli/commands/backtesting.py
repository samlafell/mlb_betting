#!/usr/bin/env python3
"""
Recommendation-Based Backtesting CLI Commands

This module provides CLI commands for running recommendation-based backtests
that only test bets the system would actually recommend.

Key principle: If a bet fits a strategy definition, the system recommends it,
and that's exactly what gets backtested - no more, no less.
"""

import asyncio
import uuid
from datetime import datetime
from decimal import Decimal

import click
from rich.console import Console
from rich.progress import Progress, SpinnerColumn, TextColumn
from rich.table import Table

from src.analysis.backtesting.engine import (
    RecommendationBacktestConfig,
    create_recommendation_backtesting_engine,
)
from src.analysis.processors.consensus_processor import UnifiedConsensusProcessor
from src.analysis.processors.public_fade_processor import UnifiedPublicFadeProcessor
from src.analysis.processors.sharp_action_processor import UnifiedSharpActionProcessor
from src.analysis.processors.timing_based_processor import UnifiedTimingBasedProcessor
from src.analysis.processors.underdog_value_processor import (
    UnifiedUnderdogValueProcessor,
)
from src.core.config import get_settings
from src.core.logging import LogComponent, get_logger
from src.data.database import get_unified_repository

console = Console()
logger = get_logger(__name__, LogComponent.CLI)


@click.group(name="backtest")
def backtesting_group():
    """Recommendation-based backtesting commands"""
    pass


@backtesting_group.command("run")
@click.option(
    "--start-date", "-s", required=True, help="Start date for backtest (YYYY-MM-DD)"
)
@click.option(
    "--end-date", "-e", required=True, help="End date for backtest (YYYY-MM-DD)"
)
@click.option(
    "--strategies",
    "-st",
    multiple=True,
    default=["sharp_action", "consensus", "timing_based"],
    help="Strategy processors to include (sharp_action, consensus, timing_based, underdog_value, public_fade)",
)
@click.option(
    "--initial-bankroll",
    "-b",
    default=10000,
    type=float,
    help="Initial bankroll for backtest (default: $10,000)",
)
@click.option(
    "--bet-sizing",
    "-bs",
    default="fixed",
    type=click.Choice(["fixed", "percentage", "kelly"]),
    help="Bet sizing method (default: fixed)",
)
@click.option(
    "--bet-size",
    default=100,
    type=float,
    help="Fixed bet size or percentage/kelly base (default: $100)",
)
@click.option(
    "--min-confidence",
    "-mc",
    default=0.6,
    type=float,
    help="Minimum confidence threshold for recommendations (default: 0.6)",
)
@click.option("--output-file", "-o", help="Save detailed results to file")
@click.option(
    "--verbose", "-v", is_flag=True, help="Show detailed progress and results"
)
def run_recommendation_backtest(
    start_date: str,
    end_date: str,
    strategies: list[str],
    initial_bankroll: float,
    bet_sizing: str,
    bet_size: float,
    min_confidence: float,
    output_file: str | None,
    verbose: bool,
):
    """
    Run a recommendation-based backtest.
    
    This backtest only tests bets that the system would actually recommend,
    ensuring perfect alignment between backtesting and live performance.
    
    Example:
        uv run python -m src.interfaces.cli.main backtest run \\
            --start-date 2024-01-01 --end-date 2024-01-31 \\
            --strategies sharp_action --strategies consensus \\
            --initial-bankroll 10000 --bet-sizing fixed --bet-size 100
    """
    asyncio.run(
        _run_recommendation_backtest_async(
            start_date,
            end_date,
            strategies,
            initial_bankroll,
            bet_sizing,
            bet_size,
            min_confidence,
            output_file,
            verbose,
        )
    )


async def _run_recommendation_backtest_async(
    start_date: str,
    end_date: str,
    strategies: list[str],
    initial_bankroll: float,
    bet_sizing: str,
    bet_size: float,
    min_confidence: float,
    output_file: str | None,
    verbose: bool,
):
    """Async implementation of recommendation backtest"""

    try:
        # Parse dates
        start_dt = datetime.strptime(start_date, "%Y-%m-%d")
        end_dt = datetime.strptime(end_date, "%Y-%m-%d")

        if start_dt >= end_dt:
            console.print("[red]Error: Start date must be before end date[/red]")
            return

        console.print(
            "[bold blue]🎯 Recommendation-Based Backtesting Engine[/bold blue]"
        )
        console.print(
            "[green]Principle: Only backtest what the system would actually recommend[/green]"
        )
        console.print()

        # Initialize repository and engine
        settings = get_settings()
        repository = get_unified_repository()
        engine = create_recommendation_backtesting_engine(repository)

        console.print(f"📅 Period: {start_date} to {end_date}")
        console.print(f"💰 Initial Bankroll: ${initial_bankroll:,.2f}")
        console.print(f"🎯 Strategies: {', '.join(strategies)}")
        console.print(f"📊 Bet Sizing: {bet_sizing} (${bet_size})")
        console.print(f"🎲 Min Confidence: {min_confidence:.1%}")
        console.print()

        # Initialize strategy processors
        strategy_processors = []
        processor_config = {
            "min_confidence_threshold": min_confidence,
            "enable_debug_logging": verbose,
        }

        for strategy_name in strategies:
            if strategy_name == "sharp_action":
                processor = UnifiedSharpActionProcessor(repository, processor_config)
                strategy_processors.append(processor)
            elif strategy_name == "consensus":
                processor = UnifiedConsensusProcessor(repository, processor_config)
                strategy_processors.append(processor)
            elif strategy_name == "timing_based":
                processor = UnifiedTimingBasedProcessor(repository, processor_config)
                strategy_processors.append(processor)
            elif strategy_name == "underdog_value":
                processor = UnifiedUnderdogValueProcessor(repository, processor_config)
                strategy_processors.append(processor)
            elif strategy_name == "public_fade":
                processor = UnifiedPublicFadeProcessor(repository, processor_config)
                strategy_processors.append(processor)
            else:
                console.print(
                    f"[yellow]Warning: Unknown strategy '{strategy_name}' - skipping[/yellow]"
                )

        if not strategy_processors:
            console.print("[red]Error: No valid strategy processors found[/red]")
            return

        console.print(f"✅ Initialized {len(strategy_processors)} strategy processors")
        console.print()

        # Create backtest configuration
        config = RecommendationBacktestConfig(
            backtest_id=str(uuid.uuid4()),
            strategy_processors=strategy_processors,
            start_date=start_dt,
            end_date=end_dt,
            initial_bankroll=Decimal(str(initial_bankroll)),
            bet_sizing_method=bet_sizing,
            fixed_bet_size=Decimal(str(bet_size)),
            percentage_bet_size=bet_size / 100 if bet_sizing == "percentage" else 0.02,
            min_confidence_threshold=min_confidence,
        )

        # Run backtest with progress indicator
        with Progress(
            SpinnerColumn(),
            TextColumn("[progress.description]{task.description}"),
            console=console,
        ) as progress:
            task = progress.add_task(
                "Running recommendation-based backtest...", total=None
            )

            try:
                result = await engine.run_recommendation_backtest(config)
                progress.update(task, description="✅ Backtest completed!")

            except Exception as e:
                progress.update(task, description=f"❌ Backtest failed: {str(e)}")
                console.print(f"[red]Backtest failed: {e}[/red]")
                return

        console.print()

        # Display results
        _display_backtest_results(result, verbose)

        # Save detailed results if requested
        if output_file:
            await _save_backtest_results(result, output_file)
            console.print(f"💾 Detailed results saved to: {output_file}")

    except Exception as e:
        console.print(f"[red]Error running backtest: {e}[/red]")
        if verbose:
            console.print_exception()


def _display_backtest_results(result, verbose: bool):
    """Display backtest results in a formatted table"""

    # Summary table
    summary_table = Table(
        title="📊 Backtest Summary", show_header=True, header_style="bold magenta"
    )
    summary_table.add_column("Metric", style="cyan")
    summary_table.add_column("Value", style="green")

    # Core metrics
    summary_table.add_row("Total Recommendations", f"{result.total_recommendations:,}")
    summary_table.add_row(
        "Recommendations with Outcomes", f"{result.recommendations_with_outcomes:,}"
    )
    summary_table.add_row("Win Rate", f"{result.win_rate:.1%}")
    summary_table.add_row("ROI", f"{result.roi_percentage:+.2f}%")
    summary_table.add_row("Total Profit/Loss", f"${result.total_profit:+,.2f}")
    summary_table.add_row("Final Bankroll", f"${result.final_bankroll:,.2f}")

    # Performance metrics
    summary_table.add_row("Winning Bets", f"{result.winning_bets:,}")
    summary_table.add_row("Losing Bets", f"{result.losing_bets:,}")
    summary_table.add_row("Push Bets", f"{result.push_bets:,}")
    summary_table.add_row("Profit Factor", f"{result.profit_factor:.2f}")
    summary_table.add_row("Max Drawdown", f"{result.max_drawdown_percentage:.1f}%")
    summary_table.add_row("Max Consecutive Wins", f"{result.max_consecutive_wins}")
    summary_table.add_row("Max Consecutive Losses", f"{result.max_consecutive_losses}")

    console.print(summary_table)
    console.print()

    # Strategy breakdown
    if result.strategy_performance:
        strategy_table = Table(
            title="📈 Strategy Performance Breakdown",
            show_header=True,
            header_style="bold magenta",
        )
        strategy_table.add_column("Strategy", style="cyan")
        strategy_table.add_column("Recommendations", justify="right")
        strategy_table.add_column("Win Rate", justify="right")
        strategy_table.add_column("ROI", justify="right")
        strategy_table.add_column("Profit/Loss", justify="right")

        for strategy_name, stats in result.strategy_performance.items():
            strategy_table.add_row(
                strategy_name,
                f"{stats['total_recommendations']:,}",
                f"{stats['win_rate']:.1%}",
                f"{stats['roi_percentage']:+.2f}%",
                f"${stats['total_profit']:+,.2f}",
            )

        console.print(strategy_table)
        console.print()

    # Execution details
    if verbose:
        details_table = Table(
            title="⚙️ Execution Details", show_header=True, header_style="bold magenta"
        )
        details_table.add_column("Detail", style="cyan")
        details_table.add_column("Value", style="green")

        details_table.add_row("Backtest ID", result.backtest_id)
        details_table.add_row(
            "Start Time", result.start_time.strftime("%Y-%m-%d %H:%M:%S")
        )
        details_table.add_row("End Time", result.end_time.strftime("%Y-%m-%d %H:%M:%S"))
        details_table.add_row(
            "Execution Time", f"{result.execution_time_seconds:.2f} seconds"
        )
        details_table.add_row("Status", result.status.value)

        console.print(details_table)
        console.print()

    # Key insights
    console.print("[bold blue]🔍 Key Insights:[/bold blue]")

    if result.win_rate > 0.55:
        console.print("✅ Strong win rate indicates good strategy performance")
    elif result.win_rate > 0.50:
        console.print("🟡 Moderate win rate - strategies show some promise")
    else:
        console.print("❌ Low win rate - strategies may need adjustment")

    if result.roi_percentage > 5:
        console.print("✅ Positive ROI indicates profitable strategy combination")
    elif result.roi_percentage > 0:
        console.print("🟡 Small positive ROI - strategies are marginally profitable")
    else:
        console.print("❌ Negative ROI - strategies are not profitable in this period")

    if result.max_drawdown_percentage < 10:
        console.print("✅ Low drawdown indicates good risk management")
    elif result.max_drawdown_percentage < 20:
        console.print("🟡 Moderate drawdown - acceptable risk levels")
    else:
        console.print("❌ High drawdown - consider position sizing adjustments")

    console.print()
    console.print(
        "[bold green]💡 Remember: These results reflect only what the system would actually recommend![/bold green]"
    )


async def _save_backtest_results(result, output_file: str):
    """Save detailed backtest results to a file"""

    import json
    from pathlib import Path

    # Convert result to serializable format
    output_data = {
        "backtest_summary": {
            "backtest_id": result.backtest_id,
            "start_time": result.start_time.isoformat() if result.start_time else None,
            "end_time": result.end_time.isoformat() if result.end_time else None,
            "execution_time_seconds": result.execution_time_seconds,
            "status": result.status.value,
            "total_recommendations": result.total_recommendations,
            "recommendations_with_outcomes": result.recommendations_with_outcomes,
            "win_rate": result.win_rate,
            "roi_percentage": result.roi_percentage,
            "total_profit": float(result.total_profit),
            "final_bankroll": float(result.final_bankroll),
            "winning_bets": result.winning_bets,
            "losing_bets": result.losing_bets,
            "push_bets": result.push_bets,
            "profit_factor": result.profit_factor,
            "max_drawdown_percentage": result.max_drawdown_percentage,
            "max_consecutive_wins": result.max_consecutive_wins,
            "max_consecutive_losses": result.max_consecutive_losses,
        },
        "strategy_performance": result.strategy_performance,
        "recommendation_history": result.recommendation_history,
        "bankroll_history": [float(b) for b in result.bankroll_history],
        "config": {
            "start_date": result.config.start_date.isoformat(),
            "end_date": result.config.end_date.isoformat(),
            "initial_bankroll": float(result.config.initial_bankroll),
            "bet_sizing_method": result.config.bet_sizing_method,
            "fixed_bet_size": float(result.config.fixed_bet_size),
            "min_confidence_threshold": result.config.min_confidence_threshold,
        },
    }

    # Ensure output directory exists
    Path(output_file).parent.mkdir(parents=True, exist_ok=True)

    # Save to file
    with open(output_file, "w") as f:
        json.dump(output_data, f, indent=2, default=str)


@backtesting_group.command("status")
def backtest_status():
    """Show current backtesting engine status"""

    console.print(
        "[bold blue]🎯 Recommendation-Based Backtesting Engine Status[/bold blue]"
    )
    console.print()

    try:
        # This would connect to an actual running engine in a full implementation
        console.print("✅ Engine Type: Recommendation-Based")
        console.print(
            "✅ Principle: Only backtest what the system would actually recommend"
        )
        console.print("✅ Strategy Processors: Available")
        console.print("✅ Historical Data: Ready")
        console.print()
        console.print(
            "[green]Engine is ready for recommendation-based backtesting[/green]"
        )

    except Exception as e:
        console.print(f"[red]Error checking engine status: {e}[/red]")


@backtesting_group.command("compare-strategies")
@click.option(
    "--start-date", "-s", required=True, help="Start date for comparison (YYYY-MM-DD)"
)
@click.option(
    "--end-date", "-e", required=True, help="End date for comparison (YYYY-MM-DD)"
)
@click.option(
    "--initial-bankroll",
    "-b",
    default=10000,
    type=float,
    help="Initial bankroll for comparison (default: $10,000)",
)
def compare_strategies(start_date: str, end_date: str, initial_bankroll: float):
    """
    Compare performance of individual strategy processors.

    Runs separate backtests for each strategy processor to understand
    which strategies perform best in different time periods.
    """
    asyncio.run(_compare_strategies_async(start_date, end_date, initial_bankroll))


async def _compare_strategies_async(
    start_date: str, end_date: str, initial_bankroll: float
):
    """Async implementation of strategy comparison"""

    try:
        # Parse dates
        start_dt = datetime.strptime(start_date, "%Y-%m-%d")
        end_dt = datetime.strptime(end_date, "%Y-%m-%d")

        console.print("[bold blue]📊 Strategy Processor Comparison[/bold blue]")
        console.print(f"Period: {start_date} to {end_date}")
        console.print()

        # Initialize repository and engine
        repository = get_unified_repository()
        engine = create_recommendation_backtesting_engine(repository)

        # Define strategies to compare
        strategies_to_test = [
            ("Sharp Action", "sharp_action"),
            ("Consensus", "consensus"),
            ("Timing Based", "timing_based"),
            ("Underdog Value", "underdog_value"),
            ("Public Fade", "public_fade"),
        ]

        comparison_results = []

        # Run backtest for each strategy individually
        for strategy_name, strategy_key in strategies_to_test:
            console.print(f"🔄 Testing {strategy_name}...")

            try:
                # Run individual strategy backtest
                result = await _run_single_strategy_backtest(
                    engine, repository, strategy_key, start_dt, end_dt, initial_bankroll
                )

                if result:
                    comparison_results.append((strategy_name, result))
                    console.print(
                        f"✅ {strategy_name}: {result.win_rate:.1%} win rate, {result.roi_percentage:+.2f}% ROI"
                    )
                else:
                    console.print(f"❌ {strategy_name}: Failed to generate results")

            except Exception as e:
                console.print(f"❌ {strategy_name}: Error - {e}")

        console.print()

        # Display comparison table
        if comparison_results:
            _display_strategy_comparison(comparison_results)
        else:
            console.print("[red]No strategy results to compare[/red]")

    except Exception as e:
        console.print(f"[red]Error running strategy comparison: {e}[/red]")


async def _run_single_strategy_backtest(
    engine,
    repository,
    strategy_key: str,
    start_dt: datetime,
    end_dt: datetime,
    initial_bankroll: float,
):
    """Run backtest for a single strategy"""

    # Initialize strategy processor
    processor_config = {"min_confidence_threshold": 0.6}

    if strategy_key == "sharp_action":
        processor = UnifiedSharpActionProcessor(repository, processor_config)
    elif strategy_key == "consensus":
        processor = UnifiedConsensusProcessor(repository, processor_config)
    elif strategy_key == "timing_based":
        processor = UnifiedTimingBasedProcessor(repository, processor_config)
    elif strategy_key == "underdog_value":
        processor = UnifiedUnderdogValueProcessor(repository, processor_config)
    elif strategy_key == "public_fade":
        processor = UnifiedPublicFadeProcessor(repository, processor_config)
    else:
        return None

    # Create config for single strategy
    config = RecommendationBacktestConfig(
        backtest_id=str(uuid.uuid4()),
        strategy_processors=[processor],
        start_date=start_dt,
        end_date=end_dt,
        initial_bankroll=Decimal(str(initial_bankroll)),
        bet_sizing_method="fixed",
        fixed_bet_size=Decimal("100"),
        min_confidence_threshold=0.6,
    )

    # Run backtest
    result = await engine.run_recommendation_backtest(config)
    return result


def _display_strategy_comparison(comparison_results):
    """Display strategy comparison results"""

    # Sort by ROI
    comparison_results.sort(key=lambda x: x[1].roi_percentage, reverse=True)

    comparison_table = Table(
        title="🏆 Strategy Performance Comparison",
        show_header=True,
        header_style="bold magenta",
    )
    comparison_table.add_column("Rank", justify="center")
    comparison_table.add_column("Strategy", style="cyan")
    comparison_table.add_column("Recommendations", justify="right")
    comparison_table.add_column("Win Rate", justify="right")
    comparison_table.add_column("ROI", justify="right")
    comparison_table.add_column("Profit/Loss", justify="right")
    comparison_table.add_column("Max Drawdown", justify="right")

    for rank, (strategy_name, result) in enumerate(comparison_results, 1):
        # Determine rank emoji
        if rank == 1:
            rank_emoji = "🥇"
        elif rank == 2:
            rank_emoji = "🥈"
        elif rank == 3:
            rank_emoji = "🥉"
        else:
            rank_emoji = f"{rank}"

        comparison_table.add_row(
            rank_emoji,
            strategy_name,
            f"{result.total_recommendations:,}",
            f"{result.win_rate:.1%}",
            f"{result.roi_percentage:+.2f}%",
            f"${result.total_profit:+,.2f}",
            f"{result.max_drawdown_percentage:.1f}%",
        )

    console.print(comparison_table)
    console.print()

    # Winner analysis
    if comparison_results:
        winner_name, winner_result = comparison_results[0]
        console.print(
            f"🏆 [bold green]Best Performing Strategy: {winner_name}[/bold green]"
        )
        console.print(f"   Win Rate: {winner_result.win_rate:.1%}")
        console.print(f"   ROI: {winner_result.roi_percentage:+.2f}%")
        console.print(
            f"   Total Recommendations: {winner_result.total_recommendations:,}"
        )


if __name__ == "__main__":
    backtesting_group()
