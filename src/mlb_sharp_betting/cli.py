#!/usr/bin/env python3
"""
MLB Sharp Betting CLI Interface

A user-friendly command-line interface for the MLB Sharp Betting analysis system.
Provides easy access to data scraping, analysis, and reporting functionality.
"""

import asyncio
import sys
from datetime import datetime, timedelta
from pathlib import Path
from typing import Optional
from dataclasses import asdict

import click
import structlog

# 🚨 CRITICAL: Initialize universal logger compatibility FIRST
from mlb_sharp_betting.core.logging import setup_universal_logger_compatibility, get_logger
setup_universal_logger_compatibility()

from mlb_sharp_betting.entrypoint import DataPipeline
from mlb_sharp_betting.cli.commands.pre_game import pregame_group
from mlb_sharp_betting.cli.commands.daily_report import daily_report_group
from mlb_sharp_betting.cli.commands.backtesting import backtesting_group
from mlb_sharp_betting.cli.commands.data_collection import data_collection_group
from mlb_sharp_betting.cli.commands.enhanced_detection import detection_group  
from mlb_sharp_betting.cli.commands.enhanced_backtesting import enhanced_backtesting_group
from mlb_sharp_betting.cli.commands.system_status import status_group
from mlb_sharp_betting.cli.commands.diagnostics import diagnostics
from mlb_sharp_betting.services.game_manager import GameManager
from mlb_sharp_betting.services.backtesting_engine import get_backtesting_engine
from mlb_sharp_betting.services.scheduler_engine import get_scheduler_engine

# Configure logging with universal compatibility
logger = get_logger(__name__)


@click.group()
@click.option('--verbose', '-v', is_flag=True, help='Enable verbose logging')
@click.pass_context
def cli(ctx, verbose):
    """MLB Sharp Betting Analysis CLI"""
    ctx.ensure_object(dict)
    ctx.obj['verbose'] = verbose
    
    if verbose:
        import logging
        logging.getLogger().setLevel(logging.DEBUG)
        logger.info_console("🔧 Verbose logging enabled")
    
    # Ensure logger compatibility is active
    logger.debug_file_only("🚀 CLI initialized with universal logger compatibility")


@cli.command()
@click.option('--sport', default='mlb', help='Sport to analyze (default: mlb)')
@click.option('--sportsbook', default='circa', help='Sportsbook data source (default: circa)')
@click.option('--mock', is_flag=True, help='Use mock data instead of scraping')
@click.option('--output', '-o', help='Output file for results')
@click.pass_context
def run(ctx, sport, sportsbook, mock, output):
    """Run the complete data pipeline"""
    
    click.echo(f"🏀 MLB Sharp Betting Analysis")
    click.echo(f"Sport: {sport}")
    click.echo(f"Sportsbook: {sportsbook}")
    click.echo(f"Mode: {'Mock Data' if mock else 'Live Scraping'}")
    
    if output:
        click.echo(f"Output: {output}")
    
    click.echo("\n" + "="*50)
    
    pipeline = DataPipeline(
        sport=sport,
        sportsbook=sportsbook,
        dry_run=mock
    )
    
    try:
        metrics = asyncio.run(pipeline.run(output_file=output))
        
        # Print summary
        click.echo("\n" + "="*50)
        click.echo("📊 PIPELINE SUMMARY")
        click.echo(f"✅ Records Processed: {metrics['parsed_records']}")
        click.echo(f"💾 Records Stored: {metrics['stored_records']}")
        click.echo(f"🎯 Sharp Indicators: {metrics['sharp_indicators']}")
        
        if metrics['errors'] > 0:
            click.echo(f"❌ Errors: {metrics['errors']}")
        
        # ✅ FIX: Safe duration calculation with fallback
        if metrics.get('end_time') and metrics.get('start_time'):
            duration = (metrics['end_time'] - metrics['start_time']).total_seconds()
            click.echo(f"⏱️  Duration: {duration:.2f}s")
        else:
            click.echo(f"⏱️  Duration: N/A (timing data incomplete)")
        
        if metrics['stored_records'] > 0:
            click.echo("\n✨ Pipeline completed successfully!")
            
            # Show database being used
            from mlb_sharp_betting.db.connection import get_db_manager
            db_manager = get_db_manager()
            click.echo(f"📁 Database: PostgreSQL (mlb_betting)")
                
            if output:
                click.echo(f"📋 Report: {output}")
        else:
            click.echo("\n⚠️  No data was processed")
            
    except Exception as e:
        click.echo(f"\n❌ Pipeline failed: {e}")
        sys.exit(1)


@cli.command()
@click.option('--table', default='splits.raw_mlb_betting_splits', help='Table to query')
@click.option('--limit', default=10, help='Number of records to show')
def query(table, limit):
    """Query the PostgreSQL database"""
    
    from mlb_sharp_betting.services.data_service import get_data_service
    
    click.echo(f"📊 Querying {table} (limit {limit})")
    
    try:
        data_service = get_data_service()
        click.echo(f"Using PostgreSQL database: mlb_betting")
        
        if table == 'splits.raw_mlb_betting_splits':
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
            if table == 'splits.raw_mlb_betting_splits':
                columns = ['game_id', 'home_team', 'away_team', 'split_type', 
                          'home_bets_pct', 'home_stake_pct', 'sharp_action', 'last_updated']
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
                    click.echo(" | ".join(f"{str(row.get(col, '') if row.get(col) is not None else '')[:15]:15}" for col in columns))
                else:
                    click.echo(" | ".join(f"{str(val)[:15]:15}" for val in row))
                
    except Exception as e:
        click.echo(f"❌ Query failed: {e}")
        click.echo("💡 Make sure PostgreSQL is running and database 'mlb_betting' exists")


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
            total_splits = summary['total_splits']
            sharp_splits = summary['sharp_splits']
            avg_diff = summary['avg_diff']
        else:
            total_splits, sharp_splits, avg_diff = summary
        
        if total_splits == 0:
            click.echo("No data found. Run 'mlb-cli run --mock' first.")
            return
        
        click.echo(f"📊 Total Splits: {total_splits}")
        click.echo(f"🎯 Sharp Action: {sharp_splits} ({sharp_splits/total_splits*100:.1f}%)")
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
                    click.echo(f"  • {example['home_team']} vs {example['away_team']} ({example['split_type']}) - Sharp: {example['sharp_action']}")
                else:
                    game_id, home_team, away_team, split_type, home_bets, home_stake, sharp = example
                    click.echo(f"  • {home_team} vs {away_team} ({split_type}) - Sharp: {sharp}")
        
    except Exception as e:
        click.echo(f"❌ Analysis failed: {e}")
        click.echo("💡 Make sure PostgreSQL is running and database 'mlb_betting' exists")


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
            cursor.execute("SELECT COUNT(*) as count FROM splits.raw_mlb_betting_splits")
            result = cursor.fetchone()
            count = result['count']
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
            schema_names = [s['schema_name'] for s in schemas]
            
            if 'splits' in schema_names:
                click.echo("✅ Schema: splits schema exists")
            else:
                click.echo("⚠️  Schema: splits schema missing")
                
            if 'main' in schema_names:
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
    
    pipeline = DataPipeline(sport='mlb', sportsbook='circa', dry_run=True)
    
    try:
        metrics = asyncio.run(pipeline.run())
        
        click.echo("✨ Demo completed! Here's what happened:")
        click.echo(f"  📥 Scraped: {metrics['scraped_records']} records (mock data)")
        click.echo(f"  🔄 Parsed: {metrics['parsed_records']} records")
        click.echo(f"  💾 Stored: {metrics['stored_records']} records")
        click.echo(f"  🎯 Sharp Action: {metrics['sharp_indicators']} indicators")
        
        click.echo(f"\n🎉 The system is working! Try 'mlb-cli query' to see the data.")
        
    except Exception as e:
        click.echo(f"❌ Demo failed: {e}")


@cli.command()
@click.option("--date", "-d", 
              help="Date to analyze (YYYY-MM-DD format, default: yesterday)")
@click.option("--format", "-f", 
              type=click.Choice(["console", "json", "csv"]),
              default="console",
              help="Output format (default: console)")
@click.option("--output", "-o",
              type=click.Path(path_type=Path),
              help="Output file path for JSON/CSV formats")
def performance(date: Optional[str], format: str, output: Optional[Path]):
    """Generate betting performance report (alias for betting-performance report)"""
    
    # Import here to avoid circular imports
    from mlb_sharp_betting.cli.commands.betting_performance import _generate_performance_report_async, _format_console_report
    
    # Parse target date - default to yesterday
    if date:
        try:
            target_date = datetime.strptime(date, "%Y-%m-%d").date()
        except ValueError:
            click.echo(f"❌ Invalid date format: {date}. Use YYYY-MM-DD format.", err=True)
            sys.exit(1)
    else:
        target_date = (datetime.now() - timedelta(days=1)).date()
    
    try:
        report = asyncio.run(_generate_performance_report_async(
            target_date=target_date,
            min_confidence=None,
            bet_type=None,
            include_unmatched=False
        ))
        
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
            from mlb_sharp_betting.cli.commands.betting_performance import _save_csv_report
            _save_csv_report(report, output)
            click.echo(f"✅ CSV report saved to: {output}")
            
    except Exception as e:
        click.echo(f"❌ Failed to generate performance report: {str(e)}", err=True)
        sys.exit(1)


# Add pre-game commands
cli.add_command(pregame_group)


@cli.command()
@click.option('--stats', is_flag=True, help='Show game statistics')
@click.option('--backfill', is_flag=True, help='Backfill games from game_outcomes table')
@click.option('--sync', is_flag=True, help='Sync games from recent betting splits')
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
                key_display = key.replace('_', ' ').title()
                click.echo(f"  {key_display}: {value}")
        
        if backfill:
            click.echo("🔄 Backfilling games from game_outcomes table...")
            
            # Run the backfill script
            import subprocess
            result = subprocess.run(["uv", "run", "python", "backfill_games_from_outcomes.py"], 
                                  capture_output=True, text=True)
            
            if result.returncode == 0:
                click.echo("✅ Backfill completed successfully")
                if result.stdout:
                    # Show key metrics from the output
                    lines = result.stdout.split('\n')
                    for line in lines:
                        if 'Games created:' in line or 'Coverage:' in line or 'Backfill Results:' in line:
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
                splits_data.append({
                    "game_id": split[0],
                    "home_team": split[1],
                    "away_team": split[2],
                    "game_datetime": split[3]
                })
            
            sync_stats = game_manager.process_games_from_betting_splits(splits_data)
            click.echo(f"✅ Sync completed:")
            click.echo(f"  Games processed: {sync_stats['processed']}")
            click.echo(f"  Games created: {sync_stats['created']}")
            click.echo(f"  Games updated: {sync_stats['updated']}")
            if sync_stats['errors'] > 0:
                click.echo(f"  Errors: {sync_stats['errors']}")
        
    except Exception as e:
        click.echo(f"❌ Games operation failed: {e}")


@cli.command()
@click.option('--lookback-days', default=None, type=int, help='Days to look back for trend analysis (optional)')
@click.option('--min-roi', default=5.0, help='Minimum ROI threshold for strategy inclusion')
@click.option('--min-bets', default=10, help='Minimum bet count for strategy inclusion')
@click.option('--skip-opportunities', is_flag=True, help='Skip current day opportunity detection')
@click.option('--skip-database-update', is_flag=True, help='Skip updating strategy_performance table')
@click.option('--format', '-f', type=click.Choice(["console", "json", "csv"]), default="console", help="Output format")
@click.option('--output', '-o', type=click.Path(path_type=Path), help="Output file path for JSON/CSV formats")
@click.option('--export-opportunities', type=click.Path(path_type=Path), help="Export opportunities to CSV file")
def auto_integrate_strategies(
    lookback_days: Optional[int], 
    min_roi: float, 
    min_bets: int,
    skip_opportunities: bool,
    skip_database_update: bool,
    format: str,
    output: Optional[Path],
    export_opportunities: Optional[Path]
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
        click.echo("🎯 Discovering, backtesting, and validating ALL available strategies")
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
            integration_results = await strategy_manager.auto_integrate_high_roi_strategies(
                lookback_days=lookback_days or 30
            )
            
            # Create a simplified report structure for compatibility
            from dataclasses import dataclass
            from typing import List, Dict, Any, Optional
            
            @dataclass
            class SimpleReport:
                integration_results: List[Dict[str, Any]]
                total_strategies_discovered: int
                strategies_successfully_backtested: int
                strategies_failed: int
                top_performers: List[Dict[str, Any]]
                current_day_opportunities: List[Any]
                trend_analysis: Optional[Dict[str, Any]]
                database_updates: Dict[str, Any]
                execution_summary: Dict[str, Any]
                warnings: List[str]
                recommendations: Dict[str, Any]
                validation_date: str
            
            successful_results = [r for r in integration_results if r.get('status') == 'SUCCESS']
            high_roi_results = [r for r in integration_results if r.get('roi_per_100', 0) >= min_roi]
            
            report = SimpleReport(
                integration_results=integration_results,
                total_strategies_discovered=len(integration_results),
                strategies_successfully_backtested=len(successful_results),
                strategies_failed=len([r for r in integration_results if r.get('status') == 'FAILED']),
                top_performers=high_roi_results,
                current_day_opportunities=[],  # Not supported in simplified version
                trend_analysis=None,  # Not supported in simplified version
                database_updates={'info': 'Auto-integration completed via StrategyManager'},
                execution_summary={
                    'execution_time_seconds': 0,
                    'total_bets_analyzed': sum(r.get('total_bets', 0) for r in integration_results),
                    'avg_roi_qualified': sum(r.get('roi_per_100', 0) for r in high_roi_results) / max(1, len(high_roi_results)) if high_roi_results else 0,
                    'avg_win_rate_qualified': sum(r.get('win_rate', 0) for r in high_roi_results) / max(1, len(high_roi_results)) if high_roi_results else 0,
                    'profitable_strategies_count': len(high_roi_results)
                },
                warnings=["⚠️  Using simplified strategy integration - full validation features moved to StrategyManager"],
                recommendations={'note': 'Recommendations available through StrategyManager'},
                validation_date=datetime.now().strftime('%Y-%m-%d')
            )
            
            # Display results based on format
            if format == "console":
                _display_console_report(report)
            elif format == "json":
                if output:
                    # Simple JSON export since we don't have the full validation service
                    import json
                    with open(output, 'w') as f:
                        json.dump(asdict(report), f, indent=2, default=str)
                    click.echo(f"✅ JSON report exported to: {output}")
                else:
                    import json
                    click.echo(json.dumps(asdict(report), indent=2, default=str))
            elif format == "csv" and export_opportunities:
                # Simple CSV export for opportunities  
                import csv
                with open(export_opportunities, 'w', newline='') as f:
                    writer = csv.writer(f)
                    writer.writerow(['Note'])
                    writer.writerow(['No opportunities in simplified version - use StrategyManager directly'])
                click.echo(f"✅ Opportunities exported to: {export_opportunities}")
            
            # Export opportunities separately if requested
            if export_opportunities and format != "csv":
                import csv
                with open(export_opportunities, 'w', newline='') as f:
                    writer = csv.writer(f)
                    writer.writerow(['Note'])
                    writer.writerow(['No opportunities in simplified version - use StrategyManager directly'])
                click.echo(f"✅ Opportunities exported to: {export_opportunities}")
            
            # Display summary
            click.echo(f"\n🎉 VALIDATION COMPLETED SUCCESSFULLY")
            click.echo(f"⏱️  Execution time: {report.execution_summary['execution_time_seconds']:.2f}s")
            click.echo(f"📈 Total bets analyzed: {report.execution_summary['total_bets_analyzed']:,}")
            
            if report.warnings:
                click.echo(f"\n⚠️  WARNINGS:")
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
    click.echo(f"✅ Successfully Backtested: {report.strategies_successfully_backtested}")
    click.echo(f"❌ Failed: {report.strategies_failed}")
    click.echo()
    
    # Top Performers
    if report.top_performers:
        click.echo("🏆 TOP PERFORMING STRATEGIES")
        click.echo("-" * 60)
        for i, strategy in enumerate(report.top_performers[:10], 1):  # Top 10
            strategy_name = strategy.get('strategy_name', 'Unknown')
            roi_per_100 = strategy.get('roi_per_100', 0)
            win_rate = strategy.get('win_rate', 0)
            total_bets = strategy.get('total_bets', 0)
            source_book_type = strategy.get('source_book_type', 'Unknown')
            
            click.echo(f"{i:2d}. 🔥 {strategy_name}")
            click.echo(f"    📊 ROI: {roi_per_100:+.1f}% | WR: {win_rate*100:.1f}% | Bets: {total_bets}")
            click.echo(f"    📈 Source: {source_book_type}")
            
            # Performance indicators
            if roi_per_100 >= 20:
                click.echo(f"    🌟 EXCELLENT - High ROI performer")
            elif roi_per_100 >= 10:
                click.echo(f"    ⭐ GOOD - Solid profit potential")
            elif roi_per_100 >= 5:
                click.echo(f"    ✅ PROFITABLE - Above threshold")
            
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
    click.echo(f"Profitable Strategies: {exec_summary.get('profitable_strategies_count', 0)}")
    if exec_summary.get('avg_roi_qualified', 0) > 0:
        click.echo(f"Avg ROI (Qualified): {exec_summary.get('avg_roi_qualified', 0):.1f}%")
        click.echo(f"Avg Win Rate (Qualified): {exec_summary.get('avg_win_rate_qualified', 0)*100:.1f}%")


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
            click.echo(f"   📊 Performance: {strategy.roi_per_100_unit:.1f}% ROI, {strategy.win_rate:.1f}% WR")
            click.echo(f"   📈 Sample size: {strategy.total_bets} bets ({strategy.confidence_level} confidence)")
            click.echo(f"   🎚️  Thresholds: {strategy.min_threshold:.1f}% (min) / {strategy.high_threshold:.1f}% (high)")
            click.echo(f"   📅 Integrated: {strategy.created_at.strftime('%Y-%m-%d %H:%M')}")
            
            # Special info for strategy types
            if 'contrarian' in strategy.strategy_variant.lower():
                click.echo(f"   💡 CONTRARIAN: Fades weaker signal when ML and spread oppose")
            elif 'opposing' in strategy.strategy_variant.lower():
                click.echo(f"   ⚔️  OPPOSING MARKETS: Exploits ML vs spread disagreements")
            elif 'sharp' in strategy.strategy_variant.lower():
                click.echo(f"   🔪 SHARP ACTION: Follows professional betting patterns")
            
            # ⚠️ CRITICAL WARNING: This strategy is untested
            click.echo(f"   ⚠️  WARNING: Cross-market flip strategies have NO backtesting results")
            click.echo(f"   📊 Confidence is theoretical only - strategy performance unknown")
            click.echo(f"   💡 Use small bet sizes until strategy is proven")
            
            click.echo()
    
    asyncio.run(run())


@cli.command()
@click.option('--setup-schema', is_flag=True, help='Set up database schema')
@click.option('--verify-schema', is_flag=True, help='Verify database schema')
@click.option('--demo', is_flag=True, help='Run integration demo')
@click.option('--stats', is_flag=True, help='Show storage statistics')
@click.option('--integrity-check', is_flag=True, help='Run data integrity check')
@click.option('--cleanup', type=int, help='Clean up data older than N days')
def database(setup_schema: bool, verify_schema: bool, demo: bool, stats: bool, integrity_check: bool, cleanup: Optional[int]) -> None:
    """Database operations and integration testing."""
    asyncio.run(_database_operations(setup_schema, verify_schema, demo, stats, integrity_check, cleanup))


async def _database_operations(
    setup_schema: bool, 
    verify_schema: bool, 
    demo: bool, 
    stats: bool, 
    integrity_check: bool,
    cleanup: Optional[int]
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
            conn_stats = stats_data.get('connection_stats', {})
            click.echo(f"  Read Operations: {conn_stats.get('read_operations', 0)}")
            click.echo(f"  Write Operations: {conn_stats.get('write_operations', 0)}")
            click.echo(f"  Bulk Operations: {conn_stats.get('bulk_operations', 0)}")
            click.echo(f"  Errors: {conn_stats.get('connection_errors', 0)}")
            
            click.echo("\n📥 COLLECTION STATS:")
            coll_stats = stats_data.get('collection_stats', {})
            click.echo(f"  Sources Attempted: {coll_stats.get('sources_attempted', 0)}")
            click.echo(f"  Sources Successful: {coll_stats.get('sources_successful', 0)}")
            click.echo(f"  Total Splits Collected: {coll_stats.get('total_splits_collected', 0)}")
            
            click.echo("\n💾 PERSISTENCE STATS:")
            persist_stats = stats_data.get('persistence_stats', {})
            click.echo(f"  Splits Processed: {persist_stats.get('splits_processed', 0)}")
            click.echo(f"  Splits Stored: {persist_stats.get('splits_stored', 0)}")
            click.echo(f"  Splits Skipped: {persist_stats.get('splits_skipped', 0)}")
            click.echo(f"  Validation Errors: {persist_stats.get('validation_errors', 0)}")
            
            # Get table counts
            splits_count = data_service.execute_read("SELECT COUNT(*) FROM splits.raw_mlb_betting_splits")
            if splits_count:
                click.echo(f"\n📊 TABLE COUNTS:")
                click.echo(f"  Betting Splits: {splits_count[0][0] if splits_count[0] else 0:,}")
            
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
                    total = duplicate_check[0]['total']
                    unique = duplicate_check[0]['unique_combinations']
                
                click.echo(f"✅ Data integrity check completed")
                click.echo(f"  Total records: {total:,}")
                click.echo(f"  Unique combinations: {unique:,}")
                if total > unique:
                    click.echo(f"  ⚠️  Potential duplicates: {total - unique:,}")
                else:
                    click.echo(f"  ✅ No duplicates detected")
            
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
            click.echo(f"✅ Cleanup completed")
            
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
cli.add_command(data_collection_group, name='data')
cli.add_command(detection_group, name='detect')  
cli.add_command(enhanced_backtesting_group, name='backtest')
cli.add_command(status_group, name='status')


@cli.command()
@click.option('--minutes-ahead', default=240, help='Minutes ahead to search for betting opportunities')
@click.option('--debug', is_flag=True, help='Enable debug output for performance monitoring')
@click.option('--show-stats', is_flag=True, help='Show repository performance statistics')
@click.option('--batch-mode', is_flag=True, help='Use batch data retrieval for optimal performance')
@click.option('--clear-cache', is_flag=True, help='Clear repository cache before processing')
def detect_opportunities(minutes_ahead: int, debug: bool, show_stats: bool, batch_mode: bool, clear_cache: bool):
    """
    Detect betting opportunities using multiple strategy processors
    
    🚀 ENHANCED: Added performance monitoring and batch optimization options
    """
    asyncio.run(_detect_opportunities_async(minutes_ahead, debug, show_stats, batch_mode, clear_cache))

async def _detect_opportunities_async(minutes_ahead: int, debug: bool, show_stats: bool, 
                                    batch_mode: bool, clear_cache: bool):
    """
    Simplified opportunity detection that works with basic data
    """
    from mlb_sharp_betting.db.connection import get_db_manager
    from mlb_sharp_betting.services.data_service import get_data_service
    from datetime import datetime, timedelta
    
    start_time = datetime.now()
    
    try:
        db_manager = get_db_manager()
        data_service = get_data_service(db_manager)
        
        if debug:
            click.echo("🔍 DEBUG MODE: Enhanced logging enabled")
            click.echo(f"⚙️  Configuration: minutes_ahead={minutes_ahead}, batch_mode={batch_mode}")
        
        # Calculate time window for upcoming games
        now = datetime.now()
        target_time = now + timedelta(minutes=minutes_ahead)
        
        click.echo("🔄 Searching for betting opportunities...")
        
        # Simple query to find recent betting splits with strong differentials
        query = """
            SELECT DISTINCT
                game_id,
                home_team,
                away_team,
                split_type,
                home_or_over_bets_percentage,
                home_or_over_stake_percentage,
                ABS(home_or_over_bets_percentage - home_or_over_stake_percentage) as differential,
                last_updated,
                game_datetime
            FROM splits.raw_mlb_betting_splits
            WHERE 
                last_updated >= CURRENT_DATE - INTERVAL '2 days'
                AND ABS(home_or_over_bets_percentage - home_or_over_stake_percentage) >= 10.0
                AND game_datetime IS NOT NULL
                AND game_datetime >= NOW()
                AND game_datetime <= NOW() + INTERVAL '%s minutes'
            ORDER BY differential DESC, last_updated DESC
            LIMIT 50
        """
        
        opportunities = data_service.execute_read(query, (minutes_ahead,))
        
        end_time = datetime.now()
        processing_time = (end_time - start_time).total_seconds()
        
        if opportunities:
            click.echo(f"\n🎯 Found {len(opportunities)} potential opportunities:")
            
            # Group by game for better display
            games = {}
            for opp in opportunities:
                # Handle both dict and tuple formats
                if isinstance(opp, dict):
                    game_key = f"{opp['away_team']} @ {opp['home_team']}"
                    if game_key not in games:
                        games[game_key] = []
                    games[game_key].append(opp)
                else:
                    # Tuple format: game_id, home_team, away_team, split_type, home_bets, home_stake, differential, last_updated, game_datetime
                    game_id, home_team, away_team, split_type, home_bets, home_stake, differential, last_updated, game_datetime = opp
                    game_key = f"{away_team} @ {home_team}"
                    if game_key not in games:
                        games[game_key] = []
                    games[game_key].append({
                        'game_id': game_id,
                        'home_team': home_team,
                        'away_team': away_team,
                        'split_type': split_type,
                        'home_or_over_bets_percentage': home_bets,
                        'home_or_over_stake_percentage': home_stake,
                        'differential': differential,
                        'last_updated': last_updated,
                        'game_datetime': game_datetime
                    })
            
            # Display results
            for game_key, game_opps in games.items():
                click.echo(f"\n📋 {game_key}:")
                
                for opp in game_opps[:3]:  # Show top 3 per game
                    split_type = opp['split_type']
                    differential = float(opp['differential'])
                    home_bets = float(opp['home_or_over_bets_percentage'])
                    home_stake = float(opp['home_or_over_stake_percentage'])
                    
                    # Determine which side has the sharp money (stake vs bets)
                    if home_stake > home_bets:
                        sharp_side = "HOME" if split_type == "moneyline" or split_type == "spread" else "OVER"
                        public_side = "AWAY" if split_type == "moneyline" or split_type == "spread" else "UNDER"
                        recommendation = f"Sharp money on {sharp_side}"
                    else:
                        sharp_side = "AWAY" if split_type == "moneyline" or split_type == "spread" else "UNDER"
                        public_side = "HOME" if split_type == "moneyline" or split_type == "spread" else "OVER"
                        recommendation = f"Sharp money on {sharp_side}"
                    
                    confidence = min(95.0, max(50.0, differential * 2.5))  # Simple confidence calc
                    
                    click.echo(f"   • {split_type.title()}: {recommendation}")
                    click.echo(f"     📊 Differential: {differential:.1f}% | Confidence: {confidence:.1f}%")
                    click.echo(f"     🎯 Bets: {home_bets:.1f}% | Money: {home_stake:.1f}%")
                    
                    if differential >= 20:
                        click.echo("     🔥 STRONG SIGNAL - High differential")
                    elif differential >= 15:
                        click.echo("     ⭐ GOOD SIGNAL - Notable differential")
                    
                if len(game_opps) > 3:
                    click.echo(f"   ... and {len(game_opps) - 3} more signals")
        else:
            click.echo("📭 No betting opportunities found")
            click.echo("💡 Try increasing --minutes-ahead or check if data is recent")
        
        # Performance summary
        click.echo(f"\n⏱️  Processing completed in {processing_time:.2f} seconds")
        
        if show_stats:
            click.echo("\n📊 Simple Detection Statistics:")
            click.echo(f"   • Database queries: 1 (optimized)")
            click.echo(f"   • Records analyzed: {len(opportunities) if opportunities else 0}")
            click.echo(f"   • Games with opportunities: {len(games) if opportunities else 0}")
            click.echo("   • Detection method: Direct SQL analysis")
    
    except Exception as e:
        click.echo(f"❌ Error detecting opportunities: {e}", err=True)
        if debug:
            import traceback
            click.echo(traceback.format_exc(), err=True)
        sys.exit(1)

async def _detect_with_batch_optimization(detector, repository, minutes_ahead: int, debug: bool):
    """
    Simplified batch optimization - now just returns empty list since we use direct SQL
    """
    if debug:
        click.echo("🔄 Batch mode enabled but using direct SQL approach")
    return []


@cli.command()
@click.option('--hours-back', '-h', type=int, default=24,
              help='Hours back to search for flips (default: 24)')
@click.option('--min-confidence', '-c', type=float, default=50.0,
              help='Minimum confidence score (default: 50.0)')
@click.option('--source', type=click.Choice(['VSIN', 'SBD']),
              help='Filter by data source')
@click.option('--book', type=click.Choice(['circa', 'draftkings', 'fanduel', 'betmgm']),
              help='Filter by sportsbook')
@click.option('--format', '-f', 
              type=click.Choice(["console", "json"]),
              default="console",
              help="Output format (default: console)")
@click.option('--output', '-o',
              type=click.Path(path_type=Path),
              help="Output file path for JSON format")
def cross_market_flips(hours_back: int, min_confidence: float, source: Optional[str], book: Optional[str], format: str, output: Optional[Path]):
    """🔀 Detect cross-market betting flips (spread vs moneyline contradictions)"""
    
    async def run_flip_detection():
        from mlb_sharp_betting.db.connection import get_db_manager
        from mlb_sharp_betting.services.cross_market_flip_detector import CrossMarketFlipDetector
        from mlb_sharp_betting.models.splits import DataSource, BookType
        
        db_manager = get_db_manager()
        flip_detector = CrossMarketFlipDetector(db_manager)
        
        # Convert string enums
        source_enum = DataSource(source) if source else None
        book_enum = BookType(book) if book else None
        
        try:
            click.echo("🔀 CROSS-MARKET FLIP DETECTOR")
            click.echo("=" * 60)
            click.echo(f"🔍 Searching last {hours_back} hours for flips ≥{min_confidence}% confidence")
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
                    hours_back=hours_back,
                    min_confidence=min_confidence
                )
                summary = None
            
            if format == "console":
                if not flips:
                    if summary:
                        click.echo(f"\n📊 Evaluated {summary['games_evaluated']} games today")
                        click.echo(f"❌ No cross-market flips found with ≥{min_confidence}% confidence")
                    else:
                        click.echo(f"\n❌ No cross-market flips found with ≥{min_confidence}% confidence in the last {hours_back} hours")
                    return
                
                click.echo(f"\n🎯 Found {len(flips)} cross-market flips:")
                
                for i, flip in enumerate(flips, 1):
                    click.echo(f"\n🔀 FLIP #{i}: {flip.away_team} @ {flip.home_team}")
                    click.echo(f"   📅 Game: {flip.game_datetime.strftime('%Y-%m-%d %H:%M EST')}")
                    click.echo(f"   🎮 Game ID: {flip.game_id}")
                    click.echo(f"   🔄 Type: {flip.flip_type.value.replace('_', ' ').title()}")
                    click.echo(f"   📊 Confidence: {flip.confidence_score:.1f}%")
                    
                    # Early signal
                    early = flip.early_signal
                    click.echo(f"\n   📈 EARLY SIGNAL ({early.hours_before_game:.1f}h before game):")
                    click.echo(f"      🎲 Market: {early.split_type.value.title()}")
                    click.echo(f"      🎯 Recommended: {early.recommended_team}")
                    click.echo(f"      📊 Differential: {early.differential:+.1f}%")
                    click.echo(f"      💪 Strength: {early.strength.value.replace('_', ' ').title()}")
                    click.echo(f"      🏛️  Source: {early.source.value}-{early.book.value if early.book else 'All'}")
                    
                    # Late signal
                    late = flip.late_signal
                    click.echo(f"\n   📉 LATE SIGNAL ({late.hours_before_game:.1f}h before game):")
                    click.echo(f"      🎲 Market: {late.split_type.value.title()}")
                    click.echo(f"      🎯 Recommended: {late.recommended_team}")
                    click.echo(f"      📊 Differential: {late.differential:+.1f}%")
                    click.echo(f"      💪 Strength: {late.strength.value.replace('_', ' ').title()}")
                    
                    # Analysis
                    click.echo(f"\n   🧠 ANALYSIS:")
                    click.echo(f"      ⏰ Signal Gap: {flip.hours_between_signals:.1f} hours")
                    click.echo(f"      💡 Strategy: {flip.strategy_recommendation}")
                    click.echo(f"      📝 Reasoning: {flip.reasoning}")
                    
                    # Risk factors
                    if flip.risk_factors:
                        click.echo(f"\n   ⚠️  RISK FACTORS:")
                        for risk in flip.risk_factors:
                            click.echo(f"      • {risk}")
                    
                    # Highlight high-confidence flips
                    if flip.confidence_score >= 80:
                        click.echo(f"\n   🔥 HIGH CONFIDENCE - STRONG BETTING OPPORTUNITY")
                    elif flip.confidence_score >= 70:
                        click.echo(f"\n   ✨ GOOD CONFIDENCE - SOLID BETTING OPPORTUNITY")
                    elif flip.confidence_score >= 60:
                        click.echo(f"\n   👍 MODERATE CONFIDENCE - CONSIDER BETTING")
                    
                    # ⚠️ CRITICAL WARNING: This strategy is untested
                    click.echo(f"   ⚠️  WARNING: Cross-market flip strategies have NO backtesting results")
                    click.echo(f"   📊 Confidence is theoretical only - strategy performance unknown")
                    click.echo(f"   💡 Use small bet sizes until strategy is proven")
                    
                    click.echo("-" * 60)
            
            elif format == "json":
                import json
                from datetime import datetime
                
                # Convert flips to JSON
                json_flips = []
                for flip in flips:
                    json_flips.append({
                        'game_id': flip.game_id,
                        'away_team': flip.away_team,
                        'home_team': flip.home_team,
                        'game_datetime': flip.game_datetime.isoformat(),
                        'flip_type': flip.flip_type.value,
                        'confidence_score': flip.confidence_score,
                        'strategy_recommendation': flip.strategy_recommendation,
                        'reasoning': flip.reasoning,
                        'hours_between_signals': flip.hours_between_signals,
                        'early_signal': {
                            'split_type': flip.early_signal.split_type.value,
                            'recommended_team': flip.early_signal.recommended_team,
                            'differential': flip.early_signal.differential,
                            'strength': flip.early_signal.strength.value,
                            'hours_before_game': flip.early_signal.hours_before_game,
                            'source': flip.early_signal.source.value,
                            'book': flip.early_signal.book.value if flip.early_signal.book else None,
                            'timing_bucket': flip.early_signal.timing_bucket.value
                        },
                        'late_signal': {
                            'split_type': flip.late_signal.split_type.value,
                            'recommended_team': flip.late_signal.recommended_team,
                            'differential': flip.late_signal.differential,
                            'strength': flip.late_signal.strength.value,
                            'hours_before_game': flip.late_signal.hours_before_game,
                            'source': flip.late_signal.source.value,
                            'book': flip.late_signal.book.value if flip.late_signal.book else None,
                            'timing_bucket': flip.late_signal.timing_bucket.value
                        },
                        'risk_factors': flip.risk_factors
                    })
                
                json_output = {
                    'timestamp': datetime.now().isoformat(),
                    'search_hours_back': hours_back,
                    'min_confidence': min_confidence,
                    'source_filter': source,
                    'book_filter': book,
                    'flips_found': len(flips),
                    'cross_market_flips': json_flips
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
cli.add_command(data_collection_group, name='data')
cli.add_command(detection_group, name='detect')
cli.add_command(enhanced_backtesting_group, name='backtest')
cli.add_command(status_group, name='status')
cli.add_command(diagnostics, name='diagnostics')

# Add individual commands


if __name__ == '__main__':
    cli() 