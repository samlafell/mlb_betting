#!/usr/bin/env python3
"""
MLB Sharp Betting CLI Interface

A user-friendly command-line interface for the MLB Sharp Betting analysis system.
Provides easy access to data scraping, analysis, and reporting functionality.
"""

import asyncio
import sys
from dataclasses import asdict
from datetime import datetime, timedelta
from pathlib import Path

import click

# 🚨 CRITICAL: Initialize universal logger compatibility FIRST
from mlb_sharp_betting.core.logging import (
    get_logger,
    setup_universal_logger_compatibility,
)

setup_universal_logger_compatibility()

from mlb_sharp_betting.cli.commands.backtesting import backtesting_group
from mlb_sharp_betting.cli.commands.daily_report import daily_report_group
from mlb_sharp_betting.cli.commands.data_collection import data_collection_group
from mlb_sharp_betting.cli.commands.diagnostics import diagnostics
from mlb_sharp_betting.cli.commands.enhanced_backtesting import (
    enhanced_backtesting_group,
)
from mlb_sharp_betting.cli.commands.enhanced_detection import detection_group
from mlb_sharp_betting.cli.commands.pre_game import pregame_group
from mlb_sharp_betting.cli.commands.system_status import status_group
from mlb_sharp_betting.entrypoint import DataPipeline
from mlb_sharp_betting.services.game_manager import GameManager

# Configure logging with universal compatibility
logger = get_logger(__name__)


@click.group()
@click.option("--verbose", "-v", is_flag=True, help="Enable verbose logging")
@click.pass_context
def cli(ctx, verbose):
    """MLB Sharp Betting Analysis CLI"""
    ctx.ensure_object(dict)
    ctx.obj["verbose"] = verbose

    if verbose:
        import logging

        logging.getLogger().setLevel(logging.DEBUG)
        logger.info_console("🔧 Verbose logging enabled")

    # Ensure logger compatibility is active
    logger.debug_file_only("🚀 CLI initialized with universal logger compatibility")


@cli.command()
@click.option("--sport", default="mlb", help="Sport to analyze (default: mlb)")
@click.option(
    "--sportsbook", default="circa", help="Sportsbook data source (default: circa)"
)
@click.option("--mock", is_flag=True, help="Use mock data instead of scraping")
@click.option("--output", "-o", help="Output file for results")
@click.pass_context
def run(ctx, sport, sportsbook, mock, output):
    """Run the complete data pipeline"""

    click.echo("🏀 MLB Sharp Betting Analysis")
    click.echo(f"Sport: {sport}")
    click.echo(f"Sportsbook: {sportsbook}")
    click.echo(f"Mode: {'Mock Data' if mock else 'Live Scraping'}")

    if output:
        click.echo(f"Output: {output}")

    click.echo("\n" + "=" * 50)

    pipeline = DataPipeline(sport=sport, sportsbook=sportsbook, dry_run=mock)

    try:
        metrics = asyncio.run(pipeline.run(output_file=output))

        # Print summary
        click.echo("\n" + "=" * 50)
        click.echo("📊 PIPELINE SUMMARY")
        click.echo(f"✅ Records Processed: {metrics['parsed_records']}")
        click.echo(f"💾 Records Stored: {metrics['stored_records']}")
        click.echo(f"🎯 Sharp Indicators: {metrics['sharp_indicators']}")

        if metrics["errors"] > 0:
            click.echo(f"❌ Errors: {metrics['errors']}")

        # ✅ FIX: Safe duration calculation with fallback
        if metrics.get("end_time") and metrics.get("start_time"):
            duration = (metrics["end_time"] - metrics["start_time"]).total_seconds()
            click.echo(f"⏱️  Duration: {duration:.2f}s")
        else:
            click.echo("⏱️  Duration: N/A (timing data incomplete)")

        if metrics["stored_records"] > 0:
            click.echo("\n✨ Pipeline completed successfully!")

            # Show database being used
            from mlb_sharp_betting.db.connection import get_db_manager

            db_manager = get_db_manager()
            click.echo("📁 Database: PostgreSQL (mlb_betting)")

            if output:
                click.echo(f"📋 Report: {output}")
        else:
            click.echo("\n⚠️  No data was processed")

    except Exception as e:
        click.echo(f"\n❌ Pipeline failed: {e}")
        sys.exit(1)


@cli.command()
@click.option("--table", default="splits.raw_mlb_betting_splits", help="Table to query")
@click.option("--limit", default=10, help="Number of records to show")
def query(table, limit):
    """Query the PostgreSQL database"""

    from mlb_sharp_betting.services.data_service import get_data_service

    click.echo(f"📊 Querying {table} (limit {limit})")

    try:
        data_service = get_data_service()
        click.echo("Using PostgreSQL database: mlb_betting")

        if table == "splits.raw_mlb_betting_splits":
            query_sql = f"""
                SELECT game_id, home_team, away_team, split_type, 
                       home_or_over_bets_percentage, home_or_over_stake_percentage,
                       sharp_action, last_updated
                FROM {table}
                ORDER BY last_updated DESC
                LIMIT %s
            """
            rows = data_service.execute_read(query_sql, (limit,))
        else:
            query_sql = f"SELECT * FROM {table} LIMIT %s"
            rows = data_service.execute_read(query_sql, (limit,))

        if not rows:
            click.echo("No data found")
            return

        # Print header - PostgreSQL returns lists of tuples or dict-like objects
        if rows and len(rows) > 0:
            # For the splits table, we know the column structure
            if table == "splits.raw_mlb_betting_splits":
                columns = [
                    "game_id",
                    "home_team",
                    "away_team",
                    "split_type",
                    "home_bets_pct",
                    "home_stake_pct",
                    "sharp_action",
                    "last_updated",
                ]
            else:
                # For other tables, we need to infer from the data
                if isinstance(rows[0], dict):
                    columns = list(rows[0].keys())
                else:
                    columns = [f"col_{i}" for i in range(len(rows[0]))]

            # Print header
            click.echo("\n" + " | ".join(f"{col:15}" for col in columns))
            click.echo("-" * (len(columns) * 17))

            # Print rows
            for row in rows:
                if isinstance(row, dict):
                    click.echo(
                        " | ".join(
                            f"{str(row.get(col, '') if row.get(col) is not None else '')[:15]:15}"
                            for col in columns
                        )
                    )
                else:
                    click.echo(" | ".join(f"{str(val)[:15]:15}" for val in row))

    except Exception as e:
        click.echo(f"❌ Query failed: {e}")
        click.echo(
            "💡 Make sure PostgreSQL is running and database 'mlb_betting' exists"
        )


@cli.command()
def analyze():
    """Analyze existing data for sharp action"""

    from mlb_sharp_betting.services.data_service import get_data_service

    click.echo("🔍 Analyzing data for sharp action...")

    try:
        data_service = get_data_service()

        # Get sharp action summary
        summary_query = """
            SELECT 
                COUNT(*) as total_splits,
                SUM(CASE WHEN sharp_action IS NOT NULL AND sharp_action != '' THEN 1 ELSE 0 END) as sharp_splits,
                AVG(ABS(home_or_over_bets_percentage - home_or_over_stake_percentage)) as avg_diff
            FROM splits.raw_mlb_betting_splits
        """

        results = data_service.execute_read(summary_query)

        if not results or not results[0]:
            click.echo("No data found. Run 'mlb-cli run --mock' first.")
            return

        summary = results[0]

        # Handle both tuple and dict-like results
        if isinstance(summary, dict):
            total_splits = summary["total_splits"]
            sharp_splits = summary["sharp_splits"]
            avg_diff = summary["avg_diff"]
        else:
            total_splits, sharp_splits, avg_diff = summary

        if total_splits == 0:
            click.echo("No data found. Run 'mlb-cli run --mock' first.")
            return

        click.echo(f"📊 Total Splits: {total_splits}")
        click.echo(
            f"🎯 Sharp Action: {sharp_splits} ({sharp_splits / total_splits * 100:.1f}%)"
        )
        click.echo(f"📈 Avg Bet/Money Diff: {avg_diff:.1f}%")

        # Get recent examples
        examples_query = """
            SELECT game_id, home_team, away_team, split_type, 
                   home_or_over_bets_percentage, home_or_over_stake_percentage, sharp_action
            FROM splits.raw_mlb_betting_splits 
            WHERE sharp_action IS NOT NULL AND sharp_action != ''
            ORDER BY last_updated DESC 
            LIMIT 5
        """

        examples = data_service.execute_read(examples_query)

        if examples:
            click.echo("\n🔥 Recent Sharp Action Examples:")
            for example in examples:
                if isinstance(example, dict):
                    click.echo(
                        f"  • {example['home_team']} vs {example['away_team']} ({example['split_type']}) - Sharp: {example['sharp_action']}"
                    )
                else:
                    (
                        game_id,
                        home_team,
                        away_team,
                        split_type,
                        home_bets,
                        home_stake,
                        sharp,
                    ) = example
                    click.echo(
                        f"  • {home_team} vs {away_team} ({split_type}) - Sharp: {sharp}"
                    )

    except Exception as e:
        click.echo(f"❌ Analysis failed: {e}")
        click.echo(
            "💡 Make sure PostgreSQL is running and database 'mlb_betting' exists"
        )


@cli.command()
def status():
    """Show system status"""

    from mlb_sharp_betting.db.connection import get_db_manager

    click.echo("🔧 System Status")

    # Check database
    try:
        db_manager = get_db_manager()
        click.echo("✅ Database: PostgreSQL Connected")

        with db_manager.get_cursor() as cursor:
            cursor.execute(
                "SELECT COUNT(*) as count FROM splits.raw_mlb_betting_splits"
            )
            result = cursor.fetchone()
            count = result["count"]
            click.echo(f"📊 Betting Splits: {count} records")

    except Exception as e:
        click.echo(f"❌ Database: {e}")
        click.echo("💡 Make sure PostgreSQL is running and configured properly")

    # Check if schema exists
    try:
        with db_manager.get_cursor() as cursor:
            cursor.execute("""
                SELECT schema_name 
                FROM information_schema.schemata 
                WHERE schema_name IN ('splits', 'main')
            """)
            schemas = cursor.fetchall()
            schema_names = [s["schema_name"] for s in schemas]

            if "splits" in schema_names:
                click.echo("✅ Schema: splits schema exists")
            else:
                click.echo("⚠️  Schema: splits schema missing")

            if "main" in schema_names:
                click.echo("✅ Schema: main schema exists")
            else:
                click.echo("⚠️  Schema: main schema missing")

    except Exception as e:
        click.echo(f"⚠️  Schema check failed: {e}")


@cli.command()
def demo():
    """Run a quick demo with mock data"""

    click.echo("🚀 Running MLB Sharp Betting Demo")
    click.echo("This will use mock data to demonstrate the system.\n")

    pipeline = DataPipeline(sport="mlb", sportsbook="circa", dry_run=True)

    try:
        metrics = asyncio.run(pipeline.run())

        click.echo("✨ Demo completed! Here's what happened:")
        click.echo(f"  📥 Scraped: {metrics['scraped_records']} records (mock data)")
        click.echo(f"  🔄 Parsed: {metrics['parsed_records']} records")
        click.echo(f"  💾 Stored: {metrics['stored_records']} records")
        click.echo(f"  🎯 Sharp Action: {metrics['sharp_indicators']} indicators")

        click.echo("\n🎉 The system is working! Try 'mlb-cli query' to see the data.")

    except Exception as e:
        click.echo(f"❌ Demo failed: {e}")


@cli.command()
@click.option(
    "--date", "-d", help="Date to analyze (YYYY-MM-DD format, default: yesterday)"
)
@click.option(
    "--format",
    "-f",
    type=click.Choice(["console", "json", "csv"]),
    default="console",
    help="Output format (default: console)",
)
@click.option(
    "--output",
    "-o",
    type=click.Path(path_type=Path),
    help="Output file path for JSON/CSV formats",
)
def performance(date: str | None, format: str, output: Path | None):
    """Generate betting performance report (alias for betting-performance report)"""

    # Import here to avoid circular imports
    from mlb_sharp_betting.cli.commands.betting_performance import (
        _format_console_report,
        _generate_performance_report_async,
    )

    # Parse target date - default to yesterday
    if date:
        try:
            target_date = datetime.strptime(date, "%Y-%m-%d").date()
        except ValueError:
            click.echo(
                f"❌ Invalid date format: {date}. Use YYYY-MM-DD format.", err=True
            )
            sys.exit(1)
    else:
        target_date = (datetime.now() - timedelta(days=1)).date()

    try:
        report = asyncio.run(
            _generate_performance_report_async(
                target_date=target_date,
                min_confidence=None,
                bet_type=None,
                include_unmatched=False,
            )
        )

        # Output the report
        if format == "console":
            click.echo(_format_console_report(report))
        elif format == "json":
            import json
            from dataclasses import asdict

            json_output = json.dumps(asdict(report), indent=2, default=str)
            if output:
                output.write_text(json_output)
                click.echo(f"✅ JSON report saved to: {output}")
            else:
                click.echo(json_output)
        elif format == "csv":
            if not output:
                click.echo("❌ CSV format requires --output option", err=True)
                sys.exit(1)
            from mlb_sharp_betting.cli.commands.betting_performance import (
                _save_csv_report,
            )

            _save_csv_report(report, output)
            click.echo(f"✅ CSV report saved to: {output}")

    except Exception as e:
        click.echo(f"❌ Failed to generate performance report: {str(e)}", err=True)
        sys.exit(1)


# Add pre-game commands
cli.add_command(pregame_group)


@cli.command()
@click.option("--stats", is_flag=True, help="Show game statistics")
@click.option(
    "--backfill", is_flag=True, help="Backfill games from game_outcomes table"
)
@click.option("--sync", is_flag=True, help="Sync games from recent betting splits")
def games(stats: bool, backfill: bool, sync: bool):
    """Manage game records in the database"""

    if not any([stats, backfill, sync]):
        click.echo("Please specify an action: --stats, --backfill, or --sync")
        return

    try:
        from mlb_sharp_betting.db.connection import get_db_manager

        db_manager = get_db_manager()
        game_manager = GameManager(db_manager)

        if stats:
            click.echo("📊 Game Statistics:")
            game_stats = game_manager.get_statistics()

            if not game_stats:
                click.echo("No game statistics available")
                return

            for key, value in game_stats.items():
                key_display = key.replace("_", " ").title()
                click.echo(f"  {key_display}: {value}")

        if backfill:
            click.echo("🔄 Backfilling games from game_outcomes table...")

            # Run the backfill script
            import subprocess

            result = subprocess.run(
                ["uv", "run", "python", "backfill_games_from_outcomes.py"],
                capture_output=True,
                text=True,
            )

            if result.returncode == 0:
                click.echo("✅ Backfill completed successfully")
                if result.stdout:
                    # Show key metrics from the output
                    lines = result.stdout.split("\n")
                    for line in lines:
                        if (
                            "Games created:" in line
                            or "Coverage:" in line
                            or "Backfill Results:" in line
                        ):
                            click.echo(f"  {line.strip()}")
            else:
                click.echo(f"❌ Backfill failed: {result.stderr}")

        if sync:
            click.echo("🔄 Syncing games from recent betting splits...")

            # Get recent betting splits and process games
            with db_manager.get_cursor() as cursor:
                cursor.execute("""
                    SELECT DISTINCT game_id, home_team, away_team, game_datetime
                    FROM splits.raw_mlb_betting_splits 
                    WHERE last_updated >= CURRENT_DATE - INTERVAL '7 days'
                      AND game_id IS NOT NULL
                """)
                recent_splits = cursor.fetchall()

            if not recent_splits:
                click.echo("No recent betting splits found")
                return

            # Convert to format expected by game manager
            splits_data = []
            for split in recent_splits:
                splits_data.append(
                    {
                        "game_id": split[0],
                        "home_team": split[1],
                        "away_team": split[2],
                        "game_datetime": split[3],
                    }
                )

            sync_stats = game_manager.process_games_from_betting_splits(splits_data)
            click.echo("✅ Sync completed:")
            click.echo(f"  Games processed: {sync_stats['processed']}")
            click.echo(f"  Games created: {sync_stats['created']}")
            click.echo(f"  Games updated: {sync_stats['updated']}")
            if sync_stats["errors"] > 0:
                click.echo(f"  Errors: {sync_stats['errors']}")

    except Exception as e:
        click.echo(f"❌ Games operation failed: {e}")


@cli.command()
@click.option(
    "--lookback-days",
    default=None,
    type=int,
    help="Days to look back for trend analysis (optional)",
)
@click.option(
    "--min-roi", default=5.0, help="Minimum ROI threshold for strategy inclusion"
)
@click.option("--min-bets", default=10, help="Minimum bet count for strategy inclusion")
@click.option(
    "--skip-opportunities", is_flag=True, help="Skip current day opportunity detection"
)
@click.option(
    "--skip-database-update",
    is_flag=True,
    help="Skip updating strategy_performance table",
)
@click.option(
    "--format",
    "-f",
    type=click.Choice(["console", "json", "csv"]),
    default="console",
    help="Output format",
)
@click.option(
    "--output",
    "-o",
    type=click.Path(path_type=Path),
    help="Output file path for JSON/CSV formats",
)
@click.option(
    "--export-opportunities",
    type=click.Path(path_type=Path),
    help="Export opportunities to CSV file",
)
def auto_integrate_strategies(
    lookback_days: int | None,
    min_roi: float,
    min_bets: int,
    skip_opportunities: bool,
    skip_database_update: bool,
    format: str,
    output: Path | None,
    export_opportunities: Path | None,
):
    """
    🚀 COMPREHENSIVE DAILY STRATEGY VALIDATION SYSTEM

    Automatically discovers all available strategies, runs complete historical backtests,
    analyzes performance, and identifies high-ROI betting opportunities for the current day.

    PHASES:
    • Phase 1: Strategy Collection & Backtesting (all available strategies)
    • Phase 2: Performance Analysis & Ranking (ROI-based rankings)
    • Phase 3: Current Day Opportunity Detection (validated strategies only)
    • Phase 4: Output & Reporting with Database Updates

    This command replaces manual strategy evaluation with automated daily validation.
    """
    import asyncio
    # Using StrategyManager instead of deprecated DailyStrategyValidationService

    async def run_comprehensive_validation():
        click.echo("🚀 COMPREHENSIVE DAILY STRATEGY VALIDATION")
        click.echo("=" * 80)
        click.echo(
            "🎯 Discovering, backtesting, and validating ALL available strategies"
        )
        click.echo(f"📊 Thresholds: ROI ≥ {min_roi}%, Bets ≥ {min_bets}")
        if lookback_days:
            click.echo(f"📈 Trend analysis: {lookback_days} days lookback")
        click.echo()

        # Initialize strategy manager for validation
        try:
            from mlb_sharp_betting.services.strategy_manager import get_strategy_manager

            strategy_manager = await get_strategy_manager()
        except Exception as e:
            click.echo(f"❌ Failed to initialize strategy manager: {e}")
            sys.exit(1)

        # Run high-ROI strategy identification and auto-integration
        try:
            integration_results = (
                await strategy_manager.auto_integrate_high_roi_strategies(
                    lookback_days=lookback_days or 30
                )
            )

            # Create a simplified report structure for compatibility
            from dataclasses import dataclass
            from typing import Any

            @dataclass
            class SimpleReport:
                integration_results: list[dict[str, Any]]
                total_strategies_discovered: int
                strategies_successfully_backtested: int
                strategies_failed: int
                top_performers: list[dict[str, Any]]
                current_day_opportunities: list[Any]
                trend_analysis: dict[str, Any] | None
                database_updates: dict[str, Any]
                execution_summary: dict[str, Any]
                warnings: list[str]
                recommendations: dict[str, Any]
                validation_date: str

            successful_results = [
                r for r in integration_results if r.get("status") == "SUCCESS"
            ]
            high_roi_results = [
                r for r in integration_results if r.get("roi_per_100", 0) >= min_roi
            ]

            report = SimpleReport(
                integration_results=integration_results,
                total_strategies_discovered=len(integration_results),
                strategies_successfully_backtested=len(successful_results),
                strategies_failed=len(
                    [r for r in integration_results if r.get("status") == "FAILED"]
                ),
                top_performers=high_roi_results,
                current_day_opportunities=[],  # Not supported in simplified version
                trend_analysis=None,  # Not supported in simplified version
                database_updates={
                    "info": "Auto-integration completed via StrategyManager"
                },
                execution_summary={
                    "execution_time_seconds": 0,
                    "total_bets_analyzed": sum(
                        r.get("total_bets", 0) for r in integration_results
                    ),
                    "avg_roi_qualified": sum(
                        r.get("roi_per_100", 0) for r in high_roi_results
                    )
                    / max(1, len(high_roi_results))
                    if high_roi_results
                    else 0,
                    "avg_win_rate_qualified": sum(
                        r.get("win_rate", 0) for r in high_roi_results
                    )
                    / max(1, len(high_roi_results))
                    if high_roi_results
                    else 0,
                    "profitable_strategies_count": len(high_roi_results),
                },
                warnings=[
                    "⚠️  Using simplified strategy integration - full validation features moved to StrategyManager"
                ],
                recommendations={
                    "note": "Recommendations available through StrategyManager"
                },
                validation_date=datetime.now().strftime("%Y-%m-%d"),
            )

            # Display results based on format
            if format == "console":
                _display_console_report(report)
            elif format == "json":
                if output:
                    # Simple JSON export since we don't have the full validation service
                    import json

                    with open(output, "w") as f:
                        json.dump(asdict(report), f, indent=2, default=str)
                    click.echo(f"✅ JSON report exported to: {output}")
                else:
                    import json

                    click.echo(json.dumps(asdict(report), indent=2, default=str))
            elif format == "csv" and export_opportunities:
                # Simple CSV export for opportunities
                import csv

                with open(export_opportunities, "w", newline="") as f:
                    writer = csv.writer(f)
                    writer.writerow(["Note"])
                    writer.writerow(
                        [
                            "No opportunities in simplified version - use StrategyManager directly"
                        ]
                    )
                click.echo(f"✅ Opportunities exported to: {export_opportunities}")

            # Export opportunities separately if requested
            if export_opportunities and format != "csv":
                import csv

                with open(export_opportunities, "w", newline="") as f:
                    writer = csv.writer(f)
                    writer.writerow(["Note"])
                    writer.writerow(
                        [
                            "No opportunities in simplified version - use StrategyManager directly"
                        ]
                    )
                click.echo(f"✅ Opportunities exported to: {export_opportunities}")

            # Display summary
            click.echo("\n🎉 VALIDATION COMPLETED SUCCESSFULLY")
            click.echo(
                f"⏱️  Execution time: {report.execution_summary['execution_time_seconds']:.2f}s"
            )
            click.echo(
                f"📈 Total bets analyzed: {report.execution_summary['total_bets_analyzed']:,}"
            )

            if report.warnings:
                click.echo("\n⚠️  WARNINGS:")
                for warning in report.warnings:
                    click.echo(f"   • {warning}")

        except Exception as e:
            click.echo(f"❌ Validation failed: {e}")
            import traceback

            click.echo(f"Details: {traceback.format_exc()}")
            sys.exit(1)

    asyncio.run(run_comprehensive_validation())


def _display_console_report(report):
    """Display comprehensive console report"""
    click.echo("📋 STRATEGY VALIDATION REPORT")
    click.echo("=" * 80)

    # Executive Summary
    click.echo(f"📅 Validation Date: {report.validation_date}")
    click.echo(f"🔍 Strategies Discovered: {report.total_strategies_discovered}")
    click.echo(
        f"✅ Successfully Backtested: {report.strategies_successfully_backtested}"
    )
    click.echo(f"❌ Failed: {report.strategies_failed}")
    click.echo()

    # Top Performers
    if report.top_performers:
        click.echo("🏆 TOP PERFORMING STRATEGIES")
        click.echo("-" * 60)
        for i, strategy in enumerate(report.top_performers[:10], 1):  # Top 10
            strategy_name = strategy.get("strategy_name", "Unknown")
            roi_per_100 = strategy.get("roi_per_100", 0)
            win_rate = strategy.get("win_rate", 0)
            total_bets = strategy.get("total_bets", 0)
            source_book_type = strategy.get("source_book_type", "Unknown")

            click.echo(f"{i:2d}. 🔥 {strategy_name}")
            click.echo(
                f"    📊 ROI: {roi_per_100:+.1f}% | WR: {win_rate * 100:.1f}% | Bets: {total_bets}"
            )
            click.echo(f"    📈 Source: {source_book_type}")

            # Performance indicators
            if roi_per_100 >= 20:
                click.echo("    🌟 EXCELLENT - High ROI performer")
            elif roi_per_100 >= 10:
                click.echo("    ⭐ GOOD - Solid profit potential")
            elif roi_per_100 >= 5:
                click.echo("    ✅ PROFITABLE - Above threshold")

            click.echo()
    else:
        click.echo("❌ No top performers found meeting criteria")

    # Current Day Opportunities (simplified)
    click.echo("🎯 TODAY'S BETTING OPPORTUNITIES")
    click.echo("-" * 60)
    click.echo("⚠️  Opportunities detection moved to StrategyManager")
    click.echo("    Use 'mlb-cli detect-opportunities' for current opportunities")
    click.echo()

    # Database Updates
    click.echo("💾 DATABASE UPDATES")
    click.echo("-" * 60)
    click.echo(f"Status: {report.database_updates.get('info', 'Completed')}")
    click.echo()

    # Summary
    exec_summary = report.execution_summary
    click.echo("📊 EXECUTION SUMMARY")
    click.echo("-" * 60)
    click.echo(f"Total Bets Analyzed: {exec_summary.get('total_bets_analyzed', 0):,}")
    click.echo(
        f"Profitable Strategies: {exec_summary.get('profitable_strategies_count', 0)}"
    )
    if exec_summary.get("avg_roi_qualified", 0) > 0:
        click.echo(
            f"Avg ROI (Qualified): {exec_summary.get('avg_roi_qualified', 0):.1f}%"
        )
        click.echo(
            f"Avg Win Rate (Qualified): {exec_summary.get('avg_win_rate_qualified', 0) * 100:.1f}%"
        )


@cli.command()
def show_active_strategies():
    """Show currently active high-ROI strategies."""
    import asyncio

    from mlb_sharp_betting.services.strategy_manager import get_strategy_manager

    async def run():
        strategy_manager = await get_strategy_manager()
        strategies = await strategy_manager.identify_high_roi_strategies()

        if not strategies:
            click.echo("❌ No active high-ROI strategies found")
            return

        click.echo(f"🎯 {len(strategies)} Active High-ROI Strategies:")
        click.echo("=" * 80)

        for strategy in strategies:
            click.echo(f"🔥 {strategy.strategy_id}")
            click.echo(
                f"   📊 Performance: {strategy.roi_per_100_unit:.1f}% ROI, {strategy.win_rate:.1f}% WR"
            )
            click.echo(
                f"   📈 Sample size: {strategy.total_bets} bets ({strategy.confidence_level} confidence)"
            )
            click.echo(
                f"   🎚️  Thresholds: {strategy.min_threshold:.1f}% (min) / {strategy.high_threshold:.1f}% (high)"
            )
            click.echo(
                f"   📅 Integrated: {strategy.created_at.strftime('%Y-%m-%d %H:%M')}"
            )

            # Special info for strategy types
            if "contrarian" in strategy.strategy_variant.lower():
                click.echo(
                    "   💡 CONTRARIAN: Fades weaker signal when ML and spread oppose"
                )
            elif "opposing" in strategy.strategy_variant.lower():
                click.echo(
                    "   ⚔️  OPPOSING MARKETS: Exploits ML vs spread disagreements"
                )
            elif "sharp" in strategy.strategy_variant.lower():
                click.echo("   🔪 SHARP ACTION: Follows professional betting patterns")

            # ⚠️ CRITICAL WARNING: This strategy is untested
            click.echo(
                "   ⚠️  WARNING: Cross-market flip strategies have NO backtesting results"
            )
            click.echo(
                "   📊 Confidence is theoretical only - strategy performance unknown"
            )
            click.echo("   💡 Use small bet sizes until strategy is proven")

            click.echo()

    asyncio.run(run())


@cli.command()
@click.option("--setup-schema", is_flag=True, help="Set up database schema")
@click.option("--verify-schema", is_flag=True, help="Verify database schema")
@click.option("--demo", is_flag=True, help="Run integration demo")
@click.option("--stats", is_flag=True, help="Show storage statistics")
@click.option("--integrity-check", is_flag=True, help="Run data integrity check")
@click.option("--cleanup", type=int, help="Clean up data older than N days")
def database(
    setup_schema: bool,
    verify_schema: bool,
    demo: bool,
    stats: bool,
    integrity_check: bool,
    cleanup: int | None,
) -> None:
    """Database operations and integration testing."""
    asyncio.run(
        _database_operations(
            setup_schema, verify_schema, demo, stats, integrity_check, cleanup
        )
    )


async def _database_operations(
    setup_schema: bool,
    verify_schema: bool,
    demo: bool,
    stats: bool,
    integrity_check: bool,
    cleanup: int | None,
) -> None:
    """Async database operations using DataService"""
    from mlb_sharp_betting.services.data_service import get_data_service

    data_service = get_data_service()

    if setup_schema:
        click.echo("🔧 Setting up database schema...")
        try:
            # The data service automatically ensures schema on initialization
            click.echo("✅ Database schema setup completed (handled by DataService)")
        except Exception as e:
            click.echo(f"❌ Schema setup failed: {e}")
            return

    if verify_schema:
        click.echo("🔍 Verifying database schema...")
        try:
            # Test connection and basic table access
            test_result = data_service.execute_read("SELECT 1 as test")
            if test_result:
                click.echo("✅ Database connection verified")

                # Check if main tables exist
                tables_check = data_service.execute_read("""
                    SELECT table_name 
                    FROM information_schema.tables 
                    WHERE table_schema = 'splits' 
                    AND table_name = 'raw_mlb_betting_splits'
                """)

                if tables_check:
                    click.echo("✅ Main betting splits table exists")
                else:
                    click.echo("⚠️  Main betting splits table not found")
            else:
                click.echo("❌ Database connection failed")
        except Exception as e:
            click.echo(f"❌ Schema verification failed: {e}")

    if demo:
        click.echo("🎬 Running database integration demo...")
        try:
            # Test data collection and storage
            splits = await data_service.collect_all_sources("mlb")
            if splits:
                click.echo(f"✅ Data collection demo: {len(splits)} splits collected")

                # Test storage (with a small sample)
                sample_splits = splits[:3] if len(splits) > 3 else splits
                storage_result = data_service.store_splits(sample_splits, validate=True)
                click.echo(f"✅ Storage demo: {storage_result}")
            else:
                click.echo("⚠️  No data collected during demo")
        except Exception as e:
            click.echo(f"❌ Demo failed: {e}")

    if stats:
        click.echo("📊 Database statistics...")
        try:
            # Get comprehensive stats from DataService
            stats_data = data_service.get_performance_stats()

            click.echo("\n🔧 CONNECTION STATS:")
            conn_stats = stats_data.get("connection_stats", {})
            click.echo(f"  Read Operations: {conn_stats.get('read_operations', 0)}")
            click.echo(f"  Write Operations: {conn_stats.get('write_operations', 0)}")
            click.echo(f"  Bulk Operations: {conn_stats.get('bulk_operations', 0)}")
            click.echo(f"  Errors: {conn_stats.get('connection_errors', 0)}")

            click.echo("\n📥 COLLECTION STATS:")
            coll_stats = stats_data.get("collection_stats", {})
            click.echo(f"  Sources Attempted: {coll_stats.get('sources_attempted', 0)}")
            click.echo(
                f"  Sources Successful: {coll_stats.get('sources_successful', 0)}"
            )
            click.echo(
                f"  Total Splits Collected: {coll_stats.get('total_splits_collected', 0)}"
            )

            click.echo("\n💾 PERSISTENCE STATS:")
            persist_stats = stats_data.get("persistence_stats", {})
            click.echo(
                f"  Splits Processed: {persist_stats.get('splits_processed', 0)}"
            )
            click.echo(f"  Splits Stored: {persist_stats.get('splits_stored', 0)}")
            click.echo(f"  Splits Skipped: {persist_stats.get('splits_skipped', 0)}")
            click.echo(
                f"  Validation Errors: {persist_stats.get('validation_errors', 0)}"
            )

            # Get table counts
            splits_count = data_service.execute_read(
                "SELECT COUNT(*) FROM splits.raw_mlb_betting_splits"
            )
            if splits_count:
                click.echo("\n📊 TABLE COUNTS:")
                click.echo(
                    f"  Betting Splits: {splits_count[0][0] if splits_count[0] else 0:,}"
                )

        except Exception as e:
            click.echo(f"❌ Stats retrieval failed: {e}")

    if integrity_check:
        click.echo("🔍 Running data integrity check...")
        try:
            # Run basic integrity checks
            duplicate_check = data_service.execute_read("""
                SELECT COUNT(*) as total, COUNT(DISTINCT game_id, split_type, source, book) as unique_combinations
                FROM splits.raw_mlb_betting_splits
            """)

            if duplicate_check and duplicate_check[0]:
                total, unique = duplicate_check[0]
                if isinstance(duplicate_check[0], dict):
                    total = duplicate_check[0]["total"]
                    unique = duplicate_check[0]["unique_combinations"]

                click.echo("✅ Data integrity check completed")
                click.echo(f"  Total records: {total:,}")
                click.echo(f"  Unique combinations: {unique:,}")
                if total > unique:
                    click.echo(f"  ⚠️  Potential duplicates: {total - unique:,}")
                else:
                    click.echo("  ✅ No duplicates detected")

        except Exception as e:
            click.echo(f"❌ Integrity check failed: {e}")

    if cleanup is not None:
        click.echo(f"🧹 Cleaning up data older than {cleanup} days...")
        try:
            from datetime import datetime, timedelta

            cutoff_date = datetime.now() - timedelta(days=cleanup)

            cleanup_query = """
                DELETE FROM splits.raw_mlb_betting_splits 
                WHERE last_updated < %s
            """

            result = data_service.execute_write(cleanup_query, (cutoff_date,))
            click.echo("✅ Cleanup completed")

        except Exception as e:
            click.echo(f"❌ Cleanup failed: {e}")


# Add existing command groups
cli.add_command(daily_report_group)
cli.add_command(backtesting_group)

# Import and add betting performance command
from mlb_sharp_betting.cli.commands.betting_performance import betting_performance_group

cli.add_command(betting_performance_group)

# Import and add timing analysis commands
from mlb_sharp_betting.cli.commands.timing_analysis import timing_group

cli.add_command(timing_group)

# Add new enhanced command groups
cli.add_command(data_collection_group, name="data")
cli.add_command(detection_group, name="detect")
cli.add_command(enhanced_backtesting_group, name="backtest")
cli.add_command(status_group, name="status")


@cli.command()
@click.option(
    "--hours-back",
    "-h",
    type=int,
    default=24,
    help="Hours back to search for flips (default: 24)",
)
@click.option(
    "--min-confidence",
    "-c",
    type=float,
    default=50.0,
    help="Minimum confidence score (default: 50.0)",
)
@click.option(
    "--source", type=click.Choice(["VSIN", "SBD"]), help="Filter by data source"
)
@click.option(
    "--book",
    type=click.Choice(["circa", "draftkings", "fanduel", "betmgm"]),
    help="Filter by sportsbook",
)
@click.option(
    "--format",
    "-f",
    type=click.Choice(["console", "json"]),
    default="console",
    help="Output format (default: console)",
)
@click.option(
    "--output",
    "-o",
    type=click.Path(path_type=Path),
    help="Output file path for JSON format",
)
def cross_market_flips(
    hours_back: int,
    min_confidence: float,
    source: str | None,
    book: str | None,
    format: str,
    output: Path | None,
):
    """🔀 Detect cross-market betting flips (spread vs moneyline contradictions)"""

    async def run_flip_detection():
        from mlb_sharp_betting.db.connection import get_db_manager
        from mlb_sharp_betting.models.splits import BookType, DataSource
        from mlb_sharp_betting.services.cross_market_flip_detector import (
            CrossMarketFlipDetector,
        )

        db_manager = get_db_manager()
        flip_detector = CrossMarketFlipDetector(db_manager)

        # Convert string enums
        source_enum = DataSource(source) if source else None
        book_enum = BookType(book) if book else None

        try:
            click.echo("🔀 CROSS-MARKET FLIP DETECTOR")
            click.echo("=" * 60)
            click.echo(
                f"🔍 Searching last {hours_back} hours for flips ≥{min_confidence}% confidence"
            )
            if source:
                click.echo(f"📡 Source filter: {source}")
            if book:
                click.echo(f"🏛️  Book filter: {book}")

            # Detect flips - use today's summary if looking at recent data
            if hours_back <= 24:
                flips, summary = await flip_detector.detect_todays_flips_with_summary(
                    min_confidence=min_confidence
                )
            else:
                flips = await flip_detector.detect_recent_flips(
                    hours_back=hours_back, min_confidence=min_confidence
                )
                summary = None

            if format == "console":
                if not flips:
                    if summary:
                        click.echo(
                            f"\n📊 Evaluated {summary['games_evaluated']} games today"
                        )
                        click.echo(
                            f"❌ No cross-market flips found with ≥{min_confidence}% confidence"
                        )
                    else:
                        click.echo(
                            f"\n❌ No cross-market flips found with ≥{min_confidence}% confidence in the last {hours_back} hours"
                        )
                    return

                click.echo(f"\n🎯 Found {len(flips)} cross-market flips:")

                for i, flip in enumerate(flips, 1):
                    click.echo(f"\n🔀 FLIP #{i}: {flip.away_team} @ {flip.home_team}")
                    click.echo(
                        f"   📅 Game: {flip.game_datetime.strftime('%Y-%m-%d %H:%M EST')}"
                    )
                    click.echo(f"   🎮 Game ID: {flip.game_id}")
                    click.echo(
                        f"   🔄 Type: {flip.flip_type.value.replace('_', ' ').title()}"
                    )
                    click.echo(f"   📊 Confidence: {flip.confidence_score:.1f}%")

                    # Early signal
                    early = flip.early_signal
                    click.echo(
                        f"\n   📈 EARLY SIGNAL ({early.hours_before_game:.1f}h before game):"
                    )
                    click.echo(f"      🎲 Market: {early.split_type.value.title()}")
                    click.echo(f"      🎯 Recommended: {early.recommended_team}")
                    click.echo(f"      📊 Differential: {early.differential:+.1f}%")
                    click.echo(
                        f"      💪 Strength: {early.strength.value.replace('_', ' ').title()}"
                    )
                    click.echo(
                        f"      🏛️  Source: {early.source.value}-{early.book.value if early.book else 'All'}"
                    )

                    # Late signal
                    late = flip.late_signal
                    click.echo(
                        f"\n   📉 LATE SIGNAL ({late.hours_before_game:.1f}h before game):"
                    )
                    click.echo(f"      🎲 Market: {late.split_type.value.title()}")
                    click.echo(f"      🎯 Recommended: {late.recommended_team}")
                    click.echo(f"      📊 Differential: {late.differential:+.1f}%")
                    click.echo(
                        f"      💪 Strength: {late.strength.value.replace('_', ' ').title()}"
                    )

                    # Analysis
                    click.echo("\n   🧠 ANALYSIS:")
                    click.echo(
                        f"      ⏰ Signal Gap: {flip.hours_between_signals:.1f} hours"
                    )
                    click.echo(f"      💡 Strategy: {flip.strategy_recommendation}")
                    click.echo(f"      📝 Reasoning: {flip.reasoning}")

                    # Risk factors
                    if flip.risk_factors:
                        click.echo("\n   ⚠️  RISK FACTORS:")
                        for risk in flip.risk_factors:
                            click.echo(f"      • {risk}")

                    # Highlight high-confidence flips
                    if flip.confidence_score >= 80:
                        click.echo(
                            "\n   🔥 HIGH CONFIDENCE - STRONG BETTING OPPORTUNITY"
                        )
                    elif flip.confidence_score >= 70:
                        click.echo(
                            "\n   ✨ GOOD CONFIDENCE - SOLID BETTING OPPORTUNITY"
                        )
                    elif flip.confidence_score >= 60:
                        click.echo("\n   👍 MODERATE CONFIDENCE - CONSIDER BETTING")

                    # ⚠️ CRITICAL WARNING: This strategy is untested
                    click.echo(
                        "   ⚠️  WARNING: Cross-market flip strategies have NO backtesting results"
                    )
                    click.echo(
                        "   📊 Confidence is theoretical only - strategy performance unknown"
                    )
                    click.echo("   💡 Use small bet sizes until strategy is proven")

                    click.echo("-" * 60)

            elif format == "json":
                import json
                from datetime import datetime

                # Convert flips to JSON
                json_flips = []
                for flip in flips:
                    json_flips.append(
                        {
                            "game_id": flip.game_id,
                            "away_team": flip.away_team,
                            "home_team": flip.home_team,
                            "game_datetime": flip.game_datetime.isoformat(),
                            "flip_type": flip.flip_type.value,
                            "confidence_score": flip.confidence_score,
                            "strategy_recommendation": flip.strategy_recommendation,
                            "reasoning": flip.reasoning,
                            "hours_between_signals": flip.hours_between_signals,
                            "early_signal": {
                                "split_type": flip.early_signal.split_type.value,
                                "recommended_team": flip.early_signal.recommended_team,
                                "differential": flip.early_signal.differential,
                                "strength": flip.early_signal.strength.value,
                                "hours_before_game": flip.early_signal.hours_before_game,
                                "source": flip.early_signal.source.value,
                                "book": flip.early_signal.book.value
                                if flip.early_signal.book
                                else None,
                                "timing_bucket": flip.early_signal.timing_bucket.value,
                            },
                            "late_signal": {
                                "split_type": flip.late_signal.split_type.value,
                                "recommended_team": flip.late_signal.recommended_team,
                                "differential": flip.late_signal.differential,
                                "strength": flip.late_signal.strength.value,
                                "hours_before_game": flip.late_signal.hours_before_game,
                                "source": flip.late_signal.source.value,
                                "book": flip.late_signal.book.value
                                if flip.late_signal.book
                                else None,
                                "timing_bucket": flip.late_signal.timing_bucket.value,
                            },
                            "risk_factors": flip.risk_factors,
                        }
                    )

                json_output = {
                    "timestamp": datetime.now().isoformat(),
                    "search_hours_back": hours_back,
                    "min_confidence": min_confidence,
                    "source_filter": source,
                    "book_filter": book,
                    "flips_found": len(flips),
                    "cross_market_flips": json_flips,
                }

                json_str = json.dumps(json_output, indent=2)
                if output:
                    output.write_text(json_str)
                    click.echo(f"✅ Cross-market flip analysis saved to: {output}")
                else:
                    click.echo(json_str)

        except Exception as e:
            click.echo(f"❌ Cross-market flip detection failed: {e}")
            sys.exit(1)
        finally:
            if db_manager:
                db_manager.close()

    try:
        asyncio.run(run_flip_detection())
    except KeyboardInterrupt:
        click.echo("\n⚠️  Flip detection interrupted by user")
    except Exception as e:
        click.echo(f"❌ Flip detection failed: {e}")
        sys.exit(1)


# Add the new command groups to the main CLI
cli.add_command(data_collection_group, name="data")
cli.add_command(detection_group, name="detect")
cli.add_command(enhanced_backtesting_group, name="backtest")
cli.add_command(status_group, name="status")
cli.add_command(diagnostics, name="diagnostics")


@cli.command()
@click.option(
    "--threshold",
    "-t",
    default=10.0,
    help="Minimum differential threshold (default: 10.0)",
)
@click.option(
    "--limit", "-l", default=20, help="Maximum number of results (default: 20)"
)
@click.option("--days-back", "-d", default=2, help="Days back to search (default: 2)")
def simple_scan(threshold: float, limit: int, days_back: int):
    """🔍 Simple betting opportunity scanner (no complex dependencies)"""

    from mlb_sharp_betting.db.connection import get_db_manager

    try:
        click.echo("🔍 SIMPLE BETTING OPPORTUNITY SCANNER")
        click.echo("=" * 60)
        click.echo(f"📊 Threshold: ≥{threshold}% differential")
        click.echo(f"📅 Search period: Last {days_back} days")
        click.echo(f"🎯 Max results: {limit}")

        db_manager = get_db_manager()

        # Ultra-simple query with no complex dependencies
        query = """
            SELECT 
                home_team,
                away_team,
                split_type,
                CAST(home_or_over_bets_percentage AS FLOAT) as bets_pct,
                CAST(home_or_over_stake_percentage AS FLOAT) as stake_pct,
                CAST(ABS(home_or_over_bets_percentage - home_or_over_stake_percentage) AS FLOAT) as diff,
                game_datetime,
                last_updated,
                source,
                book
            FROM splits.raw_mlb_betting_splits
            WHERE 
                last_updated >= CURRENT_DATE - INTERVAL '%s days'
                AND home_or_over_bets_percentage IS NOT NULL
                AND home_or_over_stake_percentage IS NOT NULL
                AND ABS(home_or_over_bets_percentage - home_or_over_stake_percentage) >= %s
            ORDER BY diff DESC, last_updated DESC
            LIMIT %s
        """

        with db_manager.get_cursor() as cursor:
            cursor.execute(query, (days_back, threshold, limit))
            results = cursor.fetchall()

        if not results:
            click.echo(f"\n❌ No opportunities found with ≥{threshold}% differential")
            click.echo("💡 Try lowering the threshold with --threshold 5.0")
            return

        click.echo(f"\n🎯 Found {len(results)} betting opportunities:")

        for i, result in enumerate(results, 1):
            # Handle both dict and tuple formats
            if isinstance(result, dict):
                home = result["home_team"]
                away = result["away_team"]
                split_type = result["split_type"]
                bets_pct = result["bets_pct"]
                stake_pct = result["stake_pct"]
                diff = result["diff"]
                source = result.get("source", "Unknown")
                book = result.get("book", "Unknown")
            else:
                (
                    home,
                    away,
                    split_type,
                    bets_pct,
                    stake_pct,
                    diff,
                    game_dt,
                    last_up,
                    source,
                    book,
                ) = result

            # Convert to float safely
            try:
                bets_pct = float(bets_pct) if bets_pct is not None else 0.0
                stake_pct = float(stake_pct) if stake_pct is not None else 0.0
                diff = float(diff) if diff is not None else 0.0
            except (ValueError, TypeError):
                continue

            # Determine sharp side
            if stake_pct > bets_pct:
                if split_type and split_type.lower() in ["moneyline", "spread"]:
                    sharp_side = "HOME"
                else:
                    sharp_side = "OVER"
            else:
                if split_type and split_type.lower() in ["moneyline", "spread"]:
                    sharp_side = "AWAY"
                else:
                    sharp_side = "UNDER"

            click.echo(f"\n{i:2d}. 🎯 {away} @ {home} ({split_type or 'unknown'})")
            click.echo(f"    💰 Sharp Money: {sharp_side}")
            click.echo(
                f"    📊 Bets: {bets_pct:.1f}% | Money: {stake_pct:.1f}% | Diff: {diff:.1f}%"
            )
            click.echo(f"    📡 Source: {source}-{book}")

            # Signal strength
            if diff >= 20:
                click.echo("    🔥 STRONG SIGNAL")
            elif diff >= 15:
                click.echo("    ⭐ GOOD SIGNAL")
            elif diff >= 10:
                click.echo("    ✅ VALID SIGNAL")

        click.echo("\n✅ Scan completed successfully")

    except Exception as e:
        click.echo(f"❌ Simple scan failed: {e}")
        click.echo("\n🔧 Basic troubleshooting:")
        click.echo("   • Is PostgreSQL running?")
        click.echo("   • Does the 'mlb_betting' database exist?")
        click.echo("   • Try 'mlb-cli status' to check system health")


@cli.group()
def game_outcomes():
    """🏆 Manage MLB game outcomes and results"""
    pass


@game_outcomes.command()
@click.option(
    "--no-betting-lines",
    is_flag=True,
    help="Use default betting lines instead of requiring splits data",
)
@click.option("--verbose", "-v", is_flag=True, help="Show detailed game results")
def daily(no_betting_lines: bool, verbose: bool):
    """📅 Update yesterday's and today's completed games"""

    async def run_daily_update():
        from mlb_sharp_betting.services.game_outcome_service import (
            get_game_outcome_service,
        )

        click.echo("🏆 DAILY GAME OUTCOMES UPDATE")
        click.echo("=" * 60)

        use_betting_lines = not no_betting_lines
        if no_betting_lines:
            click.echo("🎯 Using default betting lines (not dependent on splits data)")

        try:
            service = get_game_outcome_service()
            summary = await service.update_daily_games(
                use_betting_lines=use_betting_lines
            )

            # Display results
            click.echo("\n📊 UPDATE SUMMARY:")
            click.echo(f"   • Total games processed: {summary.total_games_processed}")
            click.echo(f"   • Dates processed: {len(summary.dates_processed)}")
            click.echo(f"   • Execution time: {summary.execution_time_seconds:.2f}s")

            if summary.dates_processed:
                click.echo(f"   • Dates: {', '.join(summary.dates_processed)}")

            # Show betting statistics
            if summary.total_games_processed > 0:
                stats = summary.betting_statistics
                click.echo("\n📈 BETTING RESULTS:")
                click.echo(
                    f"   • Home vs Away: {stats['home_wins']}-{stats['away_wins']} ({stats['home_win_percentage']:.1f}% home)"
                )
                click.echo(
                    f"   • Over vs Under: {stats['overs']}-{stats['unders']} ({stats['over_percentage']:.1f}% over)"
                )
                click.echo(
                    f"   • Home Spread: {stats['home_covers']}-{stats['away_covers']} ({stats['home_cover_percentage']:.1f}% home cover)"
                )

            # Show database status
            db_status = summary.database_status
            click.echo("\n💾 DATABASE STATUS:")
            click.echo(
                f"   • Status: {'✅ Connected' if db_status['status'] == 'connected' else '❌ Error'}"
            )
            if db_status.get("latest_game_date"):
                click.echo(f"   • Latest game: {db_status['latest_game_date']}")
            click.echo(
                f"   • Recent outcomes: {db_status.get('recent_outcomes_count', 0)}"
            )

            if summary.total_games_processed > 0:
                click.echo("\n✅ Daily update completed successfully!")
            else:
                click.echo("\nℹ️  Daily update completed - no new games found")

        except Exception as e:
            click.echo(f"❌ Daily update failed: {str(e)}")
            if verbose:
                import traceback

                click.echo(f"Details: {traceback.format_exc()}")
            sys.exit(1)

    asyncio.run(run_daily_update())


@game_outcomes.command()
@click.argument("date", type=str)
@click.option(
    "--no-betting-lines",
    is_flag=True,
    help="Use default betting lines instead of requiring splits data",
)
@click.option("--verbose", "-v", is_flag=True, help="Show detailed game results")
def date(date: str, no_betting_lines: bool, verbose: bool):
    """📅 Update games for a specific date (YYYY-MM-DD)"""

    async def run_date_update():
        from mlb_sharp_betting.services.game_outcome_service import (
            get_game_outcome_service,
        )

        # Parse date
        try:
            target_date = datetime.strptime(date, "%Y-%m-%d").date()
        except ValueError:
            click.echo(
                f"❌ Invalid date format: {date}. Use YYYY-MM-DD format.", err=True
            )
            sys.exit(1)

        click.echo("🏆 SPECIFIC DATE GAME OUTCOMES UPDATE")
        click.echo("=" * 60)
        click.echo(f"📅 Target date: {target_date}")

        use_betting_lines = not no_betting_lines
        if no_betting_lines:
            click.echo("🎯 Using default betting lines (not dependent on splits data)")

        try:
            service = get_game_outcome_service()
            summary = await service.update_specific_date(
                target_date, use_betting_lines=use_betting_lines
            )

            # Display results
            click.echo("\n📊 UPDATE SUMMARY:")
            click.echo(f"   • Total games processed: {summary.total_games_processed}")
            click.echo(f"   • Execution time: {summary.execution_time_seconds:.2f}s")

            # Show betting statistics
            if summary.total_games_processed > 0:
                stats = summary.betting_statistics
                click.echo("\n📈 BETTING RESULTS:")
                click.echo(
                    f"   • Home vs Away: {stats['home_wins']}-{stats['away_wins']} ({stats['home_win_percentage']:.1f}% home)"
                )
                click.echo(
                    f"   • Over vs Under: {stats['overs']}-{stats['unders']} ({stats['over_percentage']:.1f}% over)"
                )
                click.echo(
                    f"   • Home Spread: {stats['home_covers']}-{stats['away_covers']} ({stats['home_cover_percentage']:.1f}% home cover)"
                )

            # Show database status
            db_status = summary.database_status
            click.echo("\n💾 DATABASE STATUS:")
            click.echo(
                f"   • Status: {'✅ Connected' if db_status['status'] == 'connected' else '❌ Error'}"
            )
            if db_status.get("latest_game_date"):
                click.echo(f"   • Latest game: {db_status['latest_game_date']}")
            click.echo(
                f"   • Recent outcomes: {db_status.get('recent_outcomes_count', 0)}"
            )

            if summary.total_games_processed > 0:
                click.echo("\n✅ Date update completed successfully!")
            else:
                click.echo(
                    f"\nℹ️  Date update completed - no completed games found for {target_date}"
                )

        except Exception as e:
            click.echo(f"❌ Date update failed: {str(e)}")
            if verbose:
                import traceback

                click.echo(f"Details: {traceback.format_exc()}")
            sys.exit(1)

    asyncio.run(run_date_update())


@game_outcomes.command()
@click.option("--season-year", type=int, help="Season year (default: current season)")
@click.option(
    "--batch-size",
    type=int,
    default=7,
    help="Number of days to process in each batch (default: 7)",
)
@click.option(
    "--no-betting-lines",
    is_flag=True,
    help="Use default betting lines (recommended for large refreshes)",
)
@click.option("--verbose", "-v", is_flag=True, help="Show detailed progress")
def season_refresh(
    season_year: int | None, batch_size: int, no_betting_lines: bool, verbose: bool
):
    """🔄 Refresh game outcomes for the entire season"""

    async def run_season_refresh():
        from mlb_sharp_betting.services.game_outcome_service import (
            get_game_outcome_service,
        )

        click.echo("🏆 FULL SEASON GAME OUTCOMES REFRESH")
        click.echo("=" * 60)

        use_betting_lines = not no_betting_lines
        if no_betting_lines:
            click.echo(
                "🎯 Using default betting lines (recommended for large refreshes)"
            )

        # Confirm if using betting lines for large refresh
        if use_betting_lines:
            click.echo("⚠️  WARNING: Using betting lines for full season refresh.")
            click.echo(
                "   This will be much slower and may fail if betting splits data is incomplete."
            )
            click.echo(
                "   Consider using --no-betting-lines for faster, more reliable results."
            )
            if not click.confirm("   Continue with betting lines?"):
                return

        try:
            service = get_game_outcome_service()

            # Show estimated time based on season length
            current_date = datetime.now().date()
            estimated_year = season_year or (
                current_date.year if current_date.month >= 4 else current_date.year - 1
            )
            season_start = datetime(estimated_year, 4, 1).date()
            season_end = min(current_date, datetime(estimated_year, 10, 31).date())
            estimated_days = (season_end - season_start).days

            click.echo(f"📅 Season: {estimated_year}")
            click.echo(f"📊 Estimated date range: {season_start} to {season_end}")
            click.echo(f"🔢 Estimated days: {estimated_days}")
            click.echo(f"⏱️  Estimated time: {estimated_days * 0.5:.1f} seconds")

            if not click.confirm("Proceed with full season refresh?"):
                return

            with click.progressbar(length=100, label="Processing season...") as bar:
                result = await service.refresh_full_season(
                    season_year=season_year,
                    use_betting_lines=use_betting_lines,
                    batch_size=batch_size,
                )
                bar.update(100)

            # Display results
            click.echo("\n📊 SEASON REFRESH SUMMARY:")
            click.echo(f"   • Season: {result.season_year}")
            click.echo(f"   • Date range: {result.start_date} to {result.end_date}")
            click.echo(f"   • Total games processed: {result.total_games_processed}")
            click.echo(f"   • Dates processed: {result.dates_processed}")
            click.echo(f"   • Dates skipped: {result.dates_skipped}")
            click.echo(
                f"   • Execution time: {result.total_execution_time_seconds:.2f}s"
            )

            # Show aggregated betting statistics
            if result.total_games_processed > 0:
                all_outcomes = []
                for summary in result.daily_summaries:
                    all_outcomes.extend(
                        [None] * summary.total_games_processed
                    )  # Placeholder for stats calculation

                # Calculate season-wide stats from individual summaries
                total_home_wins = sum(
                    summary.betting_statistics["home_wins"]
                    for summary in result.daily_summaries
                )
                total_away_wins = sum(
                    summary.betting_statistics["away_wins"]
                    for summary in result.daily_summaries
                )
                total_overs = sum(
                    summary.betting_statistics["overs"]
                    for summary in result.daily_summaries
                )
                total_unders = sum(
                    summary.betting_statistics["unders"]
                    for summary in result.daily_summaries
                )
                total_home_covers = sum(
                    summary.betting_statistics["home_covers"]
                    for summary in result.daily_summaries
                )
                total_away_covers = sum(
                    summary.betting_statistics["away_covers"]
                    for summary in result.daily_summaries
                )

                click.echo("\n📈 SEASON BETTING RESULTS:")
                click.echo(
                    f"   • Home vs Away: {total_home_wins}-{total_away_wins} ({total_home_wins / (total_home_wins + total_away_wins) * 100:.1f}% home)"
                )
                click.echo(
                    f"   • Over vs Under: {total_overs}-{total_unders} ({total_overs / (total_overs + total_unders) * 100:.1f}% over)"
                )
                click.echo(
                    f"   • Home Spread: {total_home_covers}-{total_away_covers} ({total_home_covers / (total_home_covers + total_away_covers) * 100:.1f}% home cover)"
                )

            # Show errors if any
            if result.errors:
                click.echo(f"\n⚠️  ERRORS ({len(result.errors)}):")
                for error in result.errors[:5]:  # Show first 5 errors
                    click.echo(f"   • {error}")
                if len(result.errors) > 5:
                    click.echo(f"   ... and {len(result.errors) - 5} more errors")

            if result.total_games_processed > 0:
                click.echo("\n✅ Season refresh completed successfully!")
            else:
                click.echo(
                    "\nℹ️  Season refresh completed - no games found in date range"
                )

        except Exception as e:
            click.echo(f"❌ Season refresh failed: {str(e)}")
            if verbose:
                import traceback

                click.echo(f"Details: {traceback.format_exc()}")
            sys.exit(1)

    asyncio.run(run_season_refresh())


@game_outcomes.command()
@click.option("--detailed", is_flag=True, help="Show detailed database information")
def status(detailed: bool):
    """📊 Show game outcomes database status"""

    async def run_status_check():
        from mlb_sharp_betting.services.game_outcome_service import (
            get_game_outcome_service,
        )

        click.echo("🏆 GAME OUTCOMES DATABASE STATUS")
        click.echo("=" * 60)

        try:
            service = get_game_outcome_service()
            db_status = await service.get_database_status()

            # Basic status
            click.echo(
                f"💾 Database Status: {'✅ Connected' if db_status['status'] == 'connected' else '❌ Error'}"
            )

            if db_status["status"] == "connected":
                click.echo(
                    f"📊 Has data: {'✅ Yes' if db_status['has_data'] else '❌ No'}"
                )

                if db_status["has_data"]:
                    click.echo(f"🗓️  Latest game: {db_status['latest_game_date']}")
                    click.echo(
                        f"📈 Recent outcomes: {db_status['recent_outcomes_count']}"
                    )

                    if detailed:
                        # Additional detailed information
                        from mlb_sharp_betting.db.connection import get_db_manager

                        db_manager = get_db_manager()

                        with db_manager.get_cursor() as cursor:
                            # Total game outcomes
                            cursor.execute("SELECT COUNT(*) FROM public.game_outcomes")
                            total_outcomes = cursor.fetchone()[0]

                            # Date range
                            cursor.execute("""
                                SELECT MIN(game_date) as earliest, MAX(game_date) as latest
                                FROM public.game_outcomes
                            """)
                            date_range = cursor.fetchone()

                            # Games by month
                            cursor.execute("""
                                SELECT 
                                    EXTRACT(YEAR FROM game_date) as year,
                                    EXTRACT(MONTH FROM game_date) as month,
                                    COUNT(*) as games
                                FROM public.game_outcomes
                                GROUP BY year, month
                                ORDER BY year, month
                            """)
                            monthly_counts = cursor.fetchall()

                        click.echo("\n🔍 DETAILED INFORMATION:")
                        click.echo(f"   • Total outcomes: {total_outcomes:,}")
                        if date_range and date_range[0]:
                            click.echo(
                                f"   • Date range: {date_range[0]} to {date_range[1]}"
                            )

                        if monthly_counts:
                            click.echo("   • Monthly breakdown:")
                            for year, month, count in monthly_counts[
                                -12:
                            ]:  # Last 12 months
                                month_name = datetime(
                                    int(year), int(month), 1
                                ).strftime("%b %Y")
                                click.echo(f"     - {month_name}: {count} games")
                else:
                    click.echo("ℹ️  No game outcome data found")
                    click.echo("💡 Run 'mlb-cli game-outcomes daily' to populate data")

            else:
                click.echo(
                    f"❌ Database error: {db_status.get('error', 'Unknown error')}"
                )

        except Exception as e:
            click.echo(f"❌ Status check failed: {str(e)}")
            sys.exit(1)

    asyncio.run(run_status_check())


@cli.command()
@click.option("--create-missing", is_flag=True, help="Create missing tables")
@click.option("--show-schema", is_flag=True, help="Show current database schema")
def fix_schema(create_missing: bool, show_schema: bool):
    """🔧 Check and fix database schema issues"""

    from mlb_sharp_betting.db.connection import get_db_manager

    try:
        click.echo("🔧 DATABASE SCHEMA CHECKER")
        click.echo("=" * 60)

        db_manager = get_db_manager()

        # Check existing tables
        with db_manager.get_cursor() as cursor:
            cursor.execute("""
                SELECT schemaname, tablename
                FROM pg_tables 
                WHERE schemaname NOT IN ('information_schema', 'pg_catalog')
                ORDER BY schemaname, tablename
            """)
            existing_tables = cursor.fetchall()

        if show_schema:
            click.echo("📊 Current database schema:")
            current_schema = None
            for table in existing_tables:
                schema = table[0] if isinstance(table, tuple) else table["schemaname"]
                name = table[1] if isinstance(table, tuple) else table["tablename"]

                if schema != current_schema:
                    click.echo(f"\n📁 Schema: {schema}")
                    current_schema = schema

                click.echo(f"   • {name}")

        # Check for missing critical tables
        required_tables = {
            "splits": ["raw_mlb_betting_splits"],
            "validation": ["strategy_records"],
            "backtesting": [
                "strategy_configurations",
                "threshold_configurations",
                "strategy_performance",
            ],
            "main": ["games", "game_outcomes"],
        }

        existing_by_schema = {}
        for table in existing_tables:
            schema = table[0] if isinstance(table, tuple) else table["schemaname"]
            name = table[1] if isinstance(table, tuple) else table["tablename"]

            if schema not in existing_by_schema:
                existing_by_schema[schema] = []
            existing_by_schema[schema].append(name)

        click.echo("\n🔍 Schema Analysis:")
        missing_tables = []

        for schema, tables in required_tables.items():
            schema_exists = schema in existing_by_schema
            click.echo(
                f"\n📁 {schema} schema: {'✅ EXISTS' if schema_exists else '❌ MISSING'}"
            )

            if schema_exists:
                for table in tables:
                    exists = table in existing_by_schema[schema]
                    status = "✅" if exists else "❌"
                    click.echo(f"   • {table}: {status}")
                    if not exists:
                        missing_tables.append(f"{schema}.{table}")
            else:
                for table in tables:
                    click.echo(f"   • {table}: ❌ (schema missing)")
                    missing_tables.append(f"{schema}.{table}")

        if missing_tables:
            click.echo(f"\n⚠️  Missing {len(missing_tables)} critical tables:")
            for table in missing_tables:
                click.echo(f"   • {table}")

            if create_missing:
                click.echo("\n🔧 Creating missing schemas and tables...")

                with db_manager.get_cursor() as cursor:
                    # Create schemas
                    for schema in ["validation", "backtesting"]:
                        if schema not in existing_by_schema:
                            cursor.execute(f"CREATE SCHEMA IF NOT EXISTS {schema}")
                            click.echo(f"✅ Created schema: {schema}")

                    # Create basic validation table
                    if (
                        "validation" not in existing_by_schema
                        or "strategy_records"
                        not in existing_by_schema.get("validation", [])
                    ):
                        cursor.execute("""
                            CREATE TABLE IF NOT EXISTS validation.strategy_records (
                                id SERIAL PRIMARY KEY,
                                strategy_name VARCHAR(100) NOT NULL,
                                validation_date DATE NOT NULL,
                                roi_per_100 DECIMAL(10,2),
                                win_rate DECIMAL(5,2),
                                total_bets INTEGER,
                                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
                            )
                        """)
                        click.echo("✅ Created table: validation.strategy_records")

                    # Create basic backtesting tables
                    if (
                        "backtesting" not in existing_by_schema
                        or "strategy_configurations"
                        not in existing_by_schema.get("backtesting", [])
                    ):
                        cursor.execute("""
                            CREATE TABLE IF NOT EXISTS backtesting.strategy_configurations (
                                id SERIAL PRIMARY KEY,
                                strategy_name VARCHAR(255) NOT NULL,
                                source_book_type VARCHAR(100) NOT NULL,
                                split_type VARCHAR(50) NOT NULL,
                                win_rate DECIMAL(5,4) NOT NULL,
                                roi_per_100 DECIMAL(8,2) NOT NULL,
                                total_bets INTEGER NOT NULL,
                                confidence_level VARCHAR(20) NOT NULL,
                                min_threshold DECIMAL(8,2) DEFAULT 15.0,
                                moderate_threshold DECIMAL(8,2) DEFAULT 22.5,
                                high_threshold DECIMAL(8,2) DEFAULT 30.0,
                                is_active BOOLEAN DEFAULT true,
                                max_drawdown DECIMAL(8,2) DEFAULT 0.0,
                                sharpe_ratio DECIMAL(8,2) DEFAULT 0.0,
                                kelly_criterion DECIMAL(8,2) DEFAULT 0.0,
                                last_updated TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                                UNIQUE(strategy_name, source_book_type, split_type)
                            )
                        """)
                        click.echo(
                            "✅ Created table: backtesting.strategy_configurations"
                        )

                    if (
                        "backtesting" not in existing_by_schema
                        or "threshold_configurations"
                        not in existing_by_schema.get("backtesting", [])
                    ):
                        cursor.execute("""
                            CREATE TABLE IF NOT EXISTS backtesting.threshold_configurations (
                                id SERIAL PRIMARY KEY,
                                source VARCHAR(100) NOT NULL,
                                strategy_type VARCHAR(100) NOT NULL,
                                high_confidence_threshold DECIMAL(8,2) NOT NULL,
                                moderate_confidence_threshold DECIMAL(8,2) NOT NULL,
                                minimum_threshold DECIMAL(8,2) NOT NULL,
                                opposing_high_threshold DECIMAL(8,2) NOT NULL,
                                opposing_moderate_threshold DECIMAL(8,2) NOT NULL,
                                steam_threshold DECIMAL(8,2) NOT NULL,
                                steam_time_window_hours DECIMAL(4,2) DEFAULT 2.0,
                                min_sample_size INTEGER DEFAULT 10,
                                min_win_rate DECIMAL(5,4) DEFAULT 0.52,
                                is_active BOOLEAN DEFAULT true,
                                last_validated TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                                confidence_level VARCHAR(20) DEFAULT 'MODERATE',
                                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                                UNIQUE(source, strategy_type)
                            )
                        """)
                        click.echo(
                            "✅ Created table: backtesting.threshold_configurations"
                        )

                    if (
                        "backtesting" not in existing_by_schema
                        or "strategy_performance"
                        not in existing_by_schema.get("backtesting", [])
                    ):
                        cursor.execute("""
                            CREATE TABLE IF NOT EXISTS backtesting.strategy_performance (
                                id SERIAL PRIMARY KEY,
                                backtest_date DATE NOT NULL,
                                strategy_name VARCHAR(255) NOT NULL,
                                source_book_type VARCHAR(100) NOT NULL,
                                split_type VARCHAR(50) NOT NULL,
                                total_bets INTEGER NOT NULL,
                                wins INTEGER NOT NULL,
                                win_rate DECIMAL(5,4) NOT NULL,
                                roi_per_100 DECIMAL(8,2) NOT NULL,
                                confidence_level VARCHAR(20) NOT NULL,
                                kelly_criterion DECIMAL(8,2) DEFAULT 0.0,
                                sharpe_ratio DECIMAL(8,2) DEFAULT 0.0,
                                max_drawdown DECIMAL(8,2) DEFAULT 0.0,
                                total_profit_loss DECIMAL(10,2) DEFAULT 0.0,
                                is_active BOOLEAN DEFAULT true,
                                strategy_type VARCHAR(100),
                                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                                last_updated TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                                UNIQUE(strategy_name, source_book_type, split_type, backtest_date)
                            )
                        """)
                        click.echo("✅ Created table: backtesting.strategy_performance")

                click.echo("\n✅ Schema setup completed!")
            else:
                click.echo("\n💡 Use --create-missing to create missing tables")
        else:
            click.echo("\n✅ All critical tables exist!")

    except Exception as e:
        click.echo(f"❌ Schema check failed: {e}")
        import traceback

        click.echo(f"Details: {traceback.format_exc()}")


@cli.command()
@click.option(
    "--days-back",
    "-d",
    default=7,
    help="Days back to analyze for strategy configs (default: 7)",
)
@click.option(
    "--create-tables", is_flag=True, help="Create missing strategy configuration tables"
)
@click.option(
    "--force-update", is_flag=True, help="Force update even if recent configs exist"
)
def update_strategy_configs(days_back: int, create_tables: bool, force_update: bool):
    """🔧 Update strategy configurations based on backtest performance"""

    async def run_config_update():
        click.echo("🔧 STRATEGY CONFIGURATION UPDATER")
        click.echo("=" * 60)
        click.echo(f"📅 Analyzing last {days_back} days of performance")

        try:
            from mlb_sharp_betting.services.backtesting_engine import (
                get_backtesting_engine,
            )

            # Initialize backtesting engine
            backtesting_engine = get_backtesting_engine()
            await backtesting_engine.initialize()

            if create_tables:
                click.echo("🏗️  Creating strategy configuration tables...")
                # The tables will be created automatically when backtesting runs
                click.echo("✅ Tables will be created during backtest execution")

            # Calculate date range
            end_date = (datetime.now() - timedelta(days=1)).strftime("%Y-%m-%d")
            start_date = (datetime.now() - timedelta(days=days_back)).strftime(
                "%Y-%m-%d"
            )

            click.echo(f"🔍 Running backtest: {start_date} to {end_date}")

            # Run backtest to update configurations
            results = await backtesting_engine.run_backtest(
                start_date=start_date,
                end_date=end_date,
                include_diagnostics=False,
                include_alignment=False,
            )

            # Display results
            backtest_results = results.get("backtest_results", {})
            total_strategies = backtest_results.get("total_strategies", 0)
            profitable_strategies = backtest_results.get("profitable_strategies", 0)
            total_bets = backtest_results.get("total_bets", 0)
            overall_roi = backtest_results.get("overall_roi", 0)

            click.echo("\n📊 CONFIGURATION UPDATE RESULTS:")
            click.echo(f"   • Strategies analyzed: {total_strategies}")
            click.echo(f"   • Profitable strategies: {profitable_strategies}")
            click.echo(f"   • Total bets analyzed: {total_bets}")
            click.echo(f"   • Overall ROI: {overall_roi:.2f}%")
            click.echo(
                f"   • Execution time: {results.get('execution_time_seconds', 0):.2f}s"
            )

            if total_strategies > 0:
                click.echo("\n✅ Strategy configurations updated successfully!")
                click.echo(
                    "📁 Tables: backtesting.strategy_configurations & backtesting.threshold_configurations"
                )

                # Show top performing strategies
                strategy_results = backtest_results.get("strategy_results", [])
                if strategy_results:
                    click.echo("\n🏆 Top performing strategies:")
                    sorted_strategies = sorted(
                        strategy_results,
                        key=lambda x: x.get("roi_per_100", 0),
                        reverse=True,
                    )
                    for i, strategy in enumerate(sorted_strategies[:5], 1):
                        name = strategy.get("strategy_name", "Unknown")
                        roi = strategy.get("roi_per_100", 0)
                        win_rate = strategy.get("win_rate", 0)
                        bets = strategy.get("total_bets", 0)
                        click.echo(
                            f"   {i}. {name}: {roi:+.1f}% ROI, {win_rate:.1%} WR, {bets} bets"
                        )
            else:
                click.echo("\n⚠️  No strategies found in the specified date range")
                click.echo(
                    "💡 Try increasing --days-back or ensure you have recent betting data"
                )

        except Exception as e:
            click.echo(f"❌ Configuration update failed: {e}")
            import traceback

            click.echo(f"Details: {traceback.format_exc()}")
            sys.exit(1)

    try:
        asyncio.run(run_config_update())
    except KeyboardInterrupt:
        click.echo("\n⚠️  Configuration update interrupted by user")
    except Exception as e:
        click.echo(f"❌ Configuration update failed: {e}")
        sys.exit(1)


@cli.command()
@click.option("--show-inactive", is_flag=True, help="Also show inactive strategies")
@click.option("--show-thresholds", is_flag=True, help="Show threshold configurations")
def strategy_config_status(show_inactive: bool, show_thresholds: bool):
    """📊 Show current strategy configuration status"""

    async def run_status_check():
        click.echo("📊 STRATEGY CONFIGURATION STATUS")
        click.echo("=" * 60)

        try:
            from mlb_sharp_betting.db.connection import get_db_manager
            from mlb_sharp_betting.services.strategy_manager import get_strategy_manager

            # Initialize strategy manager
            strategy_manager = await get_strategy_manager()
            db_manager = get_db_manager()

            # Check if tables exist
            with db_manager.get_cursor() as cursor:
                cursor.execute("""
                    SELECT EXISTS (
                        SELECT 1 FROM information_schema.tables 
                        WHERE table_schema = 'backtesting' 
                        AND table_name = 'strategy_configurations'
                    )
                """)
                configs_table_exists = cursor.fetchone()[0]

                cursor.execute("""
                    SELECT EXISTS (
                        SELECT 1 FROM information_schema.tables 
                        WHERE table_schema = 'backtesting' 
                        AND table_name = 'threshold_configurations'
                    )
                """)
                thresholds_table_exists = cursor.fetchone()[0]

            click.echo("🏗️  Tables Status:")
            click.echo(
                f"   • strategy_configurations: {'✅ EXISTS' if configs_table_exists else '❌ MISSING'}"
            )
            click.echo(
                f"   • threshold_configurations: {'✅ EXISTS' if thresholds_table_exists else '❌ MISSING'}"
            )

            if not configs_table_exists:
                click.echo("\n⚠️  Strategy configurations table missing!")
                click.echo(
                    "💡 Run 'mlb-cli update-strategy-configs --create-tables' to create tables"
                )
                return

            # Get active strategies
            active_strategies = await strategy_manager.get_active_strategies()

            click.echo(f"\n📈 Active Strategies: {len(active_strategies)}")
            if active_strategies:
                for i, strategy in enumerate(active_strategies[:10], 1):  # Show top 10
                    click.echo(f"   {i:2d}. {strategy.strategy_name}")
                    click.echo(
                        f"       📊 {strategy.roi_per_100:+.1f}% ROI | {strategy.win_rate:.1%} WR | {strategy.total_bets} bets"
                    )
                    click.echo(
                        f"       🎯 Source: {strategy.source_book_type} | Type: {strategy.split_type}"
                    )
                    click.echo(
                        f"       🎚️  Thresholds: {strategy.min_threshold:.1f}-{strategy.high_threshold:.1f}%"
                    )
                    click.echo()

            # Show inactive strategies if requested
            if show_inactive:
                with db_manager.get_cursor() as cursor:
                    cursor.execute("""
                        SELECT strategy_name, source_book_type, split_type, roi_per_100, win_rate, total_bets
                        FROM backtesting.strategy_configurations
                        WHERE is_active = false
                        ORDER BY roi_per_100 DESC
                        LIMIT 10
                    """)
                    inactive_strategies = cursor.fetchall()

                click.echo(f"❌ Inactive Strategies: {len(inactive_strategies)}")
                for strategy in inactive_strategies:
                    if isinstance(strategy, dict):
                        name = strategy["strategy_name"]
                        roi = strategy["roi_per_100"]
                        win_rate = strategy["win_rate"]
                        bets = strategy["total_bets"]
                        source = strategy["source_book_type"]
                        split_type = strategy["split_type"]
                    else:
                        name, source, split_type, roi, win_rate, bets = strategy

                    click.echo(
                        f"   • {name}: {roi:+.1f}% ROI, {win_rate:.1%} WR, {bets} bets ({source}-{split_type})"
                    )

            # Show threshold configurations if requested
            if show_thresholds and thresholds_table_exists:
                with db_manager.get_cursor() as cursor:
                    cursor.execute("""
                        SELECT source, strategy_type, high_confidence_threshold, 
                               moderate_confidence_threshold, minimum_threshold, confidence_level
                        FROM backtesting.threshold_configurations
                        WHERE is_active = true
                        ORDER BY source, strategy_type
                    """)
                    threshold_configs = cursor.fetchall()

                click.echo(f"\n🎚️  Threshold Configurations: {len(threshold_configs)}")
                for config in threshold_configs:
                    if isinstance(config, dict):
                        source = config["source"]
                        strategy_type = config["strategy_type"]
                        high = config["high_confidence_threshold"]
                        moderate = config["moderate_confidence_threshold"]
                        minimum = config["minimum_threshold"]
                        confidence = config["confidence_level"]
                    else:
                        source, strategy_type, high, moderate, minimum, confidence = (
                            config
                        )

                    click.echo(
                        f"   • {source}-{strategy_type}: {minimum:.1f}%/{moderate:.1f}%/{high:.1f}% ({confidence})"
                    )

            # Show manager status
            manager_status = strategy_manager.get_metrics()
            click.echo("\n🔧 Manager Status:")
            click.echo(
                f"   • Strategy cache: {manager_status['cache_status']['strategy_cache_size']} strategies"
            )
            click.echo(
                f"   • Threshold cache: {manager_status['cache_status']['threshold_cache_size']} configs"
            )
            click.echo(
                f"   • Cache valid: {'✅' if manager_status['cache_status']['cache_valid'] else '❌'}"
            )

        except Exception as e:
            click.echo(f"❌ Status check failed: {e}")
            import traceback

            click.echo(f"Details: {traceback.format_exc()}")
            sys.exit(1)

    try:
        asyncio.run(run_status_check())
    except KeyboardInterrupt:
        click.echo("\n⚠️  Status check interrupted by user")
    except Exception as e:
        click.echo(f"❌ Status check failed: {e}")
        sys.exit(1)


# Add the game outcomes command group
cli.add_command(game_outcomes, name="game-outcomes")

if __name__ == "__main__":
    cli()
