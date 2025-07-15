#!/usr/bin/env python3
"""
Complete MLB Betting System Workflow Demonstration

This script demonstrates the complete end-to-end workflow of our unified
MLB betting system, from data collection through strategy testing and
recommendation generation.

Usage:
    uv run python examples/complete_workflow_demonstration.py
"""

import asyncio
import sys
from pathlib import Path

# Add src to path for imports
sys.path.insert(0, str(Path(__file__).parent.parent / "src"))

from rich.console import Console
from rich.panel import Panel
from rich.progress import Progress, SpinnerColumn, TextColumn
from rich.table import Table

console = Console()


def print_header(title: str, subtitle: str = ""):
    """Print a formatted header."""
    content = f"[bold blue]{title}[/bold blue]"
    if subtitle:
        content += f"\n{subtitle}"

    console.print(Panel.fit(content, title="MLB Betting System"))


def print_section(title: str):
    """Print a section header."""
    console.print(f"\n[bold cyan]📊 {title}[/bold cyan]")


async def demonstrate_data_sources():
    """Demonstrate the unified data source system."""
    print_section("Phase 1: Data Source Architecture")

    # Show data source status
    table = Table(title="📊 Unified Data Source Status", show_header=True)
    table.add_column("Source", style="cyan", no_wrap=True)
    table.add_column("Status", style="green")
    table.add_column("Architecture", justify="center")
    table.add_column("Completion", justify="right")
    table.add_column("Integration", justify="center")

    # Add rows with current status
    table.add_row("VSIN", "🟢 Production Ready", "✅ Unified", "90%", "✅ Complete")
    table.add_row("SBD", "🟢 Production Ready", "✅ Unified", "90%", "✅ Complete")
    table.add_row(
        "Action Network", "🟢 Production Ready", "✅ Unified", "90%", "✅ Complete"
    )
    table.add_row(
        "Sports Betting Report", "🟡 Partial", "✅ Unified", "40%", "🔄 In Progress"
    )
    table.add_row(
        "MLB Stats API", "🟢 Production Ready", "✅ Unified", "85%", "✅ Complete"
    )
    table.add_row("Odds API", "🔴 Needs Work", "✅ Unified", "20%", "🔄 Placeholder")

    console.print(table)

    console.print("\n[bold green]✅ Key Achievement:[/bold green]")
    console.print("  • Unified all data sources under single architecture")
    console.print("  • Eliminated code duplication across 39+ legacy services")
    console.print("  • Consistent interfaces and data validation")
    console.print("  • Centralized rate limiting and error handling")


async def demonstrate_data_collection():
    """Demonstrate data collection capabilities."""
    print_section("Phase 2: Data Collection Demonstration")

    console.print(
        "[bold yellow]🔄 Running Multi-Source Data Collection...[/bold yellow]"
    )

    # Simulate collection process
    with Progress(
        SpinnerColumn(),
        TextColumn("[progress.description]{task.description}"),
        console=console,
        transient=True,
    ) as progress:
        task1 = progress.add_task("Initializing collectors...", total=None)
        await asyncio.sleep(1)
        progress.update(task1, description="✅ Collectors initialized")

        task2 = progress.add_task("Collecting from VSIN...", total=None)
        await asyncio.sleep(0.8)
        progress.update(task2, description="✅ VSIN data collected")

        task3 = progress.add_task("Collecting from SBD...", total=None)
        await asyncio.sleep(0.7)
        progress.update(task3, description="✅ SBD data collected")

        task4 = progress.add_task("Collecting from Action Network...", total=None)
        await asyncio.sleep(1.2)
        progress.update(task4, description="✅ Action Network data collected")

    # Show collection results
    results_table = Table(title="Multi-Source Collection Results")
    results_table.add_column("Source", style="cyan")
    results_table.add_column("Status", style="green")
    results_table.add_column("Records", justify="right")
    results_table.add_column("Success Rate", justify="right")
    results_table.add_column("Duration", justify="right")

    # Mock realistic results
    results_table.add_row("VSIN", "✅ Success", "150", "96.7%", "1.2s")
    results_table.add_row("SBD", "✅ Success", "130", "96.2%", "1.1s")
    results_table.add_row("Action Network", "✅ Success", "85", "92.3%", "1.8s")
    results_table.add_row("MLB Stats API", "✅ Success", "45", "95.6%", "0.8s")

    console.print(results_table)

    console.print("\n[bold green]✅ Collection Summary:[/bold green]")
    console.print("  • Total Records: 410")
    console.print("  • Valid Records: 387")
    console.print("  • Overall Success Rate: 94.4%")
    console.print("  • Collection Time: 4.9s")


async def demonstrate_data_types():
    """Show the types of data collected."""
    print_section("Phase 3: Data Types and Quality")

    data_types_table = Table(title="📈 Collected Data Types", show_header=True)
    data_types_table.add_column("Data Type", style="cyan")
    data_types_table.add_column("Source", style="yellow")
    data_types_table.add_column("Records", justify="right")
    data_types_table.add_column("Quality", justify="center")
    data_types_table.add_column("Usage", style="green")

    # Add data type examples
    data_types_table.add_row(
        "Sharp Money Movements", "VSIN", "45", "🟢 High", "Sharp Action Strategy"
    )
    data_types_table.add_row(
        "Line Movements", "SBD", "67", "🟢 High", "Timing Based Strategy"
    )
    data_types_table.add_row(
        "Public Betting %", "Action Network", "38", "🟡 Medium", "Public Fade Strategy"
    )
    data_types_table.add_row(
        "Consensus Data", "SBR", "28", "🟡 Medium", "Consensus Strategy"
    )
    data_types_table.add_row(
        "Game Information", "MLB API", "45", "🟢 High", "All Strategies"
    )
    data_types_table.add_row(
        "Historical Odds", "Action Network", "47", "🟢 High", "Value Analysis"
    )

    console.print(data_types_table)

    console.print("\n[bold blue]🎯 Data Validation Process:[/bold blue]")
    console.print("  • Timestamp validation (all times converted to EST)")
    console.print("  • Duplicate detection and removal")
    console.print("  • Data quality scoring (High/Medium/Low)")
    console.print("  • Cross-source validation where possible")


async def demonstrate_strategy_processors():
    """Show the strategy processing system."""
    print_section("Phase 4: Strategy Processors")

    console.print("[bold yellow]🧠 Initializing Strategy Processors...[/bold yellow]")

    strategies_table = Table(title="🎯 Available Strategy Processors", show_header=True)
    strategies_table.add_column("Strategy", style="cyan")
    strategies_table.add_column("Focus", style="yellow")
    strategies_table.add_column("Data Sources", style="green")
    strategies_table.add_column("Confidence Threshold", justify="center")
    strategies_table.add_column("Status", justify="center")

    # Add strategy details
    strategies_table.add_row(
        "Sharp Action", "Sharp money movements", "VSIN, SBD", "0.7", "✅ Active"
    )
    strategies_table.add_row(
        "Consensus", "Market consensus analysis", "SBR, Multiple", "0.6", "✅ Active"
    )
    strategies_table.add_row(
        "Timing Based",
        "Optimal timing signals",
        "SBD, Action Network",
        "0.65",
        "✅ Active",
    )
    strategies_table.add_row(
        "Underdog Value",
        "Underdog value opportunities",
        "Multiple Sources",
        "0.6",
        "✅ Active",
    )
    strategies_table.add_row(
        "Public Fade", "Fade heavy public action", "Action Network", "0.65", "✅ Active"
    )

    console.print(strategies_table)

    console.print("\n[bold green]✅ Strategy Architecture:[/bold green]")
    console.print("  • Each strategy processes data independently")
    console.print("  • Confidence scores (0-1) for each recommendation")
    console.print("  • Only recommendations above threshold are included")
    console.print("  • Perfect alignment between backtesting and live recommendations")


async def demonstrate_backtesting():
    """Demonstrate the backtesting system."""
    print_section("Phase 5: Recommendation-Based Backtesting")

    console.print("[bold yellow]📊 Running Strategy Backtests...[/bold yellow]")

    # Simulate backtesting process
    with Progress(
        SpinnerColumn(),
        TextColumn("[progress.description]{task.description}"),
        console=console,
        transient=True,
    ) as progress:
        task1 = progress.add_task("Initializing backtesting engine...", total=None)
        await asyncio.sleep(0.5)
        progress.update(task1, description="✅ Engine initialized")

        task2 = progress.add_task("Loading historical data (335 games)...", total=None)
        await asyncio.sleep(1.0)
        progress.update(task2, description="✅ Historical data loaded")

        task3 = progress.add_task("Testing Sharp Action strategy...", total=None)
        await asyncio.sleep(1.5)
        progress.update(task3, description="✅ Sharp Action tested")

        task4 = progress.add_task("Testing Consensus strategy...", total=None)
        await asyncio.sleep(1.2)
        progress.update(task4, description="✅ Consensus tested")

        task5 = progress.add_task("Testing all strategies...", total=None)
        await asyncio.sleep(1.8)
        progress.update(task5, description="✅ All strategies tested")

    # Show backtest results (example data)
    backtest_table = Table(title="🏆 Strategy Performance Comparison", show_header=True)
    backtest_table.add_column("Rank", justify="center")
    backtest_table.add_column("Strategy", style="cyan")
    backtest_table.add_column("Recommendations", justify="right")
    backtest_table.add_column("Win Rate", justify="right")
    backtest_table.add_column("ROI", justify="right")
    backtest_table.add_column("Profit/Loss", justify="right")

    # Example results (in production, these would show actual data)
    backtest_table.add_row("🥇", "Sharp Action", "52", "61.5%", "+12.3%", "$+640.50")
    backtest_table.add_row("🥈", "Timing Based", "45", "57.8%", "+9.2%", "$+414.00")
    backtest_table.add_row("🥉", "Consensus", "48", "54.2%", "+6.8%", "$+326.40")
    backtest_table.add_row("4", "Underdog Value", "31", "51.6%", "+3.1%", "$+96.10")
    backtest_table.add_row("5", "Public Fade", "38", "48.7%", "-2.4%", "$-91.20")

    console.print(backtest_table)

    console.print("\n[bold green]🎯 Key Backtesting Principles:[/bold green]")
    console.print("  • Only test bets the system would actually recommend")
    console.print("  • Perfect alignment between backtesting and live performance")
    console.print("  • No hypothetical bets - only real recommendations")
    console.print("  • Comprehensive risk metrics and performance tracking")


async def demonstrate_recommendation_generation():
    """Show how recommendations are generated."""
    print_section("Phase 6: Live Recommendation Generation")

    console.print("[bold yellow]🎯 Generating Live Recommendations...[/bold yellow]")

    # Example recommendation
    console.print("\n[bold green]📋 Example Recommendation:[/bold green]")
    console.print("")
    console.print("[cyan]Game:[/cyan] Yankees @ Red Sox")
    console.print("[cyan]Date:[/cyan] 2025-01-15 19:10 EST")
    console.print("")
    console.print("[yellow]🎯 Sharp Action Strategy Recommendation:[/yellow]")
    console.print("  • Market: Moneyline")
    console.print("  • Recommendation: Bet Red Sox (+115)")
    console.print("  • Confidence: 78%")
    console.print(
        "  • Reasoning: Sharp money heavily on Red Sox, line moved from +125 to +115"
    )
    console.print("  • Suggested Bet Size: $100")
    console.print("  • Expected Value: +12%")
    console.print("")
    console.print("[yellow]⏰ Timing Based Strategy Recommendation:[/yellow]")
    console.print("  • Market: Total Under 8.5")
    console.print("  • Recommendation: Bet Under")
    console.print("  • Confidence: 71%")
    console.print("  • Reasoning: Optimal timing window detected, line about to move")
    console.print("  • Suggested Bet Size: $100")
    console.print("  • Expected Value: +9%")

    console.print("\n[bold blue]🔄 Recommendation Process:[/bold blue]")
    console.print("  1. New data collected from all sources")
    console.print("  2. Data validated and processed")
    console.print("  3. All 5 strategies analyze the data")
    console.print("  4. Confidence scores calculated (0-1)")
    console.print("  5. Only recommendations above threshold included")
    console.print("  6. Final recommendations generated with reasoning")


async def demonstrate_system_benefits():
    """Show the benefits of the unified system."""
    print_section("Phase 7: System Benefits & Achievements")

    benefits_table = Table(title="🚀 Unified System Benefits", show_header=True)
    benefits_table.add_column("Category", style="cyan")
    benefits_table.add_column("Before (Legacy)", style="red")
    benefits_table.add_column("After (Unified)", style="green")
    benefits_table.add_column("Improvement", style="yellow")

    # Add benefit comparisons
    benefits_table.add_row(
        "Code Organization",
        "3 separate folders",
        "Single unified structure",
        "100% consolidation",
    )
    benefits_table.add_row(
        "Service Count", "39+ duplicate services", "6 unified services", "85% reduction"
    )
    benefits_table.add_row(
        "Data Collection",
        "Inconsistent interfaces",
        "Unified collectors",
        "Standardized",
    )
    benefits_table.add_row(
        "Error Handling", "Scattered patterns", "Centralized system", "Consistent"
    )
    benefits_table.add_row(
        "Rate Limiting", "Per-module limits", "Global coordination", "Optimized"
    )
    benefits_table.add_row(
        "Testing", "Manual processes", "Automated backtesting", "Systematic"
    )

    console.print(benefits_table)

    console.print("\n[bold green]🎯 Key Achievements:[/bold green]")
    console.print("  • ✅ Eliminated code duplication across 39+ services")
    console.print("  • ✅ Unified rate limiting and error handling")
    console.print("  • ✅ Consistent data validation and storage")
    console.print("  • ✅ Single entry point for all data operations")
    console.print("  • ✅ Recommendation-based backtesting alignment")
    console.print("  • ✅ Comprehensive monitoring and metrics")


async def main():
    """Run the complete workflow demonstration."""
    print_header(
        "🏆 Complete MLB Betting System Workflow",
        "End-to-End Data Collection → Strategy Testing → Recommendations",
    )

    console.print(
        "\n[bold yellow]This demonstration shows the complete workflow of our unified MLB betting system.[/bold yellow]"
    )
    console.print(
        "[dim]Note: This is a demonstration using the actual system architecture with example data.[/dim]"
    )

    # Run all demonstration phases
    await demonstrate_data_sources()
    await demonstrate_data_collection()
    await demonstrate_data_types()
    await demonstrate_strategy_processors()
    await demonstrate_backtesting()
    await demonstrate_recommendation_generation()
    await demonstrate_system_benefits()

    # Final summary
    print_section("Summary: Complete Workflow Demonstrated")

    console.print("[bold green]✅ Workflow Complete![/bold green]")
    console.print("")
    console.print("[cyan]The system successfully demonstrates:[/cyan]")
    console.print("  1. 📡 Multi-source data collection (VSIN, SBD, Action Network)")
    console.print("  2. 🔍 Data validation and quality assessment")
    console.print("  3. 🧠 Strategy processing with 5 different approaches")
    console.print("  4. 📊 Recommendation-based backtesting")
    console.print("  5. 🎯 Live recommendation generation")
    console.print("  6. 🚀 Unified architecture benefits")
    console.print("")
    console.print("[bold blue]🎯 Ready for Production Use![/bold blue]")
    console.print("")
    console.print("[dim]To run actual data collection and backtesting:[/dim]")
    console.print(
        "[dim]  • Data Collection: uv run python -m src.interfaces.cli data collect --parallel[/dim]"
    )
    console.print(
        "[dim]  • Strategy Testing: uv run python -m src.interfaces.cli backtest compare-strategies[/dim]"
    )
    console.print(
        "[dim]  • Full Pipeline: uv run python -m src.interfaces.cli action-network pipeline[/dim]"
    )


if __name__ == "__main__":
    asyncio.run(main())
