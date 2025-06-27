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
from mlb_sharp_betting.cli.commands.orchestrator_demo import orchestrator_demo
from mlb_sharp_betting.services.game_manager import GameManager

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
        
        duration = (metrics['end_time'] - metrics['start_time']).total_seconds()
        click.echo(f"⏱️  Duration: {duration:.2f}s")
        
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
    
    from mlb_sharp_betting.db.connection import get_db_manager
    
    click.echo(f"📊 Querying {table} (limit {limit})")
    
    try:
        db_manager = get_db_manager()
        click.echo(f"Using PostgreSQL database: mlb_betting")
        
        with db_manager.get_cursor() as cursor:
            if table == 'splits.raw_mlb_betting_splits':
                cursor.execute(f"""
                    SELECT game_id, home_team, away_team, split_type, 
                           home_or_over_bets_percentage, home_or_over_stake_percentage,
                           sharp_action, last_updated
                    FROM {table}
                    ORDER BY last_updated DESC
                    LIMIT %s
                """, (limit,))
            else:
                cursor.execute(f"SELECT * FROM {table} LIMIT %s", (limit,))
            
            rows = cursor.fetchall()
            
            # Get column names from cursor description
            columns = [desc[0] for desc in cursor.description]
            
            if not rows:
                click.echo("No data found")
                return
            
            # Print header
            click.echo("\n" + " | ".join(f"{col:15}" for col in columns))
            click.echo("-" * (len(columns) * 17))
            
            # Print rows - PostgreSQL returns DictRow objects
            for row in rows:
                if hasattr(row, 'keys'):  # DictRow
                    click.echo(" | ".join(f"{str(row[col] if row[col] is not None else '')[:15]:15}" for col in columns))
                else:  # Regular tuple
                    click.echo(" | ".join(f"{str(val)[:15]:15}" for val in row))
                
    except Exception as e:
        click.echo(f"❌ Query failed: {e}")
        click.echo("💡 Make sure PostgreSQL is running and database 'mlb_betting' exists")


@cli.command()
def analyze():
    """Analyze existing data for sharp action"""
    
    from mlb_sharp_betting.db.connection import get_db_manager
    
    click.echo("🔍 Analyzing data for sharp action...")
    
    try:
        db_manager = get_db_manager()
        
        with db_manager.get_cursor() as cursor:
            # Get sharp action summary
            cursor.execute("""
                SELECT 
                    COUNT(*) as total_splits,
                    SUM(CASE WHEN sharp_action IS NOT NULL AND sharp_action != '' THEN 1 ELSE 0 END) as sharp_splits,
                    AVG(ABS(home_or_over_bets_percentage - home_or_over_stake_percentage)) as avg_diff
                FROM splits.raw_mlb_betting_splits
            """)
            
            summary = cursor.fetchone()
            
            if summary['total_splits'] == 0:
                click.echo("No data found. Run 'mlb-cli run --mock' first.")
                return
            
            click.echo(f"📊 Total Splits: {summary['total_splits']}")
            click.echo(f"🎯 Sharp Action: {summary['sharp_splits']} ({summary['sharp_splits']/summary['total_splits']*100:.1f}%)")
            click.echo(f"📈 Avg Bet/Money Diff: {summary['avg_diff']:.1f}%")
            
            # Get top sharp indicators
            cursor.execute("""
                SELECT game_id, home_team, away_team, split_type,
                       home_or_over_bets_percentage, home_or_over_stake_percentage,
                       ABS(home_or_over_bets_percentage - home_or_over_stake_percentage) as diff,
                       sharp_action
                FROM splits.raw_mlb_betting_splits
                WHERE sharp_action IS NOT NULL AND sharp_action != ''
                ORDER BY diff DESC
                LIMIT 5
            """)
            
            sharp_games = cursor.fetchall()
            
            if sharp_games:
                click.echo("\n🔥 Top Sharp Action Games:")
                for game in sharp_games:
                    click.echo(f"  {game['home_team']} vs {game['away_team']} ({game['split_type']}): {game['diff']:.1f}% difference - Sharp: {game['sharp_action']}")
        
    except Exception as e:
        click.echo(f"❌ Analysis failed: {e}")


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
@click.option('--lookback-days', default=30, help='Days to look back for strategy performance')
@click.option('--min-roi', default=10.0, help='Minimum ROI threshold for auto-integration')
@click.option('--min-bets', default=10, help='Minimum bet count for auto-integration')
def auto_integrate_strategies(lookback_days: int, min_roi: float, min_bets: int):
    """Auto-integrate high-ROI strategies into live recommendations."""
    import asyncio
    from mlb_sharp_betting.services.strategy_auto_integration import StrategyAutoIntegration
    
    async def run():
        click.echo("🎯 Starting auto-integration of high-ROI strategies...")
        
        integration_service = StrategyAutoIntegration(
            min_roi_threshold=min_roi,
            min_bet_count=min_bets
        )
        
        # Run auto-integration
        results = await integration_service.auto_integrate_high_roi_strategies(lookback_days)
        
        if not results:
            click.echo("❌ No high-ROI strategies found for integration")
            return
        
        # Display results
        successful = [r for r in results if r.integration_successful]
        failed = [r for r in results if not r.integration_successful]
        
        click.echo(f"\n📊 Auto-Integration Results:")
        click.echo(f"   ✅ Successfully integrated: {len(successful)}")
        click.echo(f"   ❌ Failed to integrate: {len(failed)}")
        
        if successful:
            click.echo(f"\n🎯 Successfully Integrated Strategies:")
            for result in successful:
                s = result.strategy
                click.echo(f"   🔥 {s.strategy_id}")
                click.echo(f"      📊 {s.roi_per_100_unit:.1f}% ROI, {s.win_rate:.1f}% WR, {s.total_bets} bets")
                click.echo(f"      🎚️  Thresholds: {s.min_threshold:.1f}% (min) / {s.high_threshold:.1f}% (high)")
                
                # Special highlighting for contrarian strategies
                if 'contrarian' in s.strategy_variant.lower():
                    click.echo(f"      💡 CONTRARIAN STRATEGY - Fades weaker signal in opposing markets")
                
                # ⚠️ CRITICAL WARNING: This strategy is untested
                click.echo(f"   ⚠️  WARNING: Cross-market flip strategies have NO backtesting results")
                click.echo(f"   📊 Confidence is theoretical only - strategy performance unknown")
                click.echo(f"   💡 Use small bet sizes until strategy is proven")
                
                click.echo()
        
        if failed:
            click.echo(f"\n⚠️  Failed Integrations:")
            for result in failed:
                click.echo(f"   ❌ {result.strategy.strategy_id}: {result.error_message}")
        
        # Show metrics
        metrics = integration_service.get_metrics()
        click.echo(f"\n📈 Integration Metrics:")
        click.echo(f"   📋 Strategies evaluated: {metrics['strategies_evaluated']}")
        click.echo(f"   🔥 Contrarian strategies found: {metrics['contrarian_strategies_found']}")
        click.echo(f"   ⚔️  Opposing markets strategies found: {metrics['opposing_markets_strategies_found']}")
    
    asyncio.run(run())


@cli.command()
def show_active_strategies():
    """Show currently active high-ROI strategies."""
    import asyncio
    from mlb_sharp_betting.services.strategy_auto_integration import StrategyAutoIntegration
    
    async def run():
        integration_service = StrategyAutoIntegration()
        strategies = await integration_service.get_active_high_roi_strategies()
        
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
    """Handle database operations."""
    from mlb_sharp_betting.db.schema import SchemaManager
    from mlb_sharp_betting.services.data_persistence import DataPersistenceService
    from mlb_sharp_betting.examples.database_integration_demo import DatabaseIntegrationDemo
    
    from mlb_sharp_betting.db.connection import get_db_manager
    persistence_service = DataPersistenceService(get_db_manager())
    schema_manager = SchemaManager()
    
    if setup_schema:
        click.echo("🔧 Setting up PostgreSQL database schema...")
        try:
            schema_manager.setup_complete_schema()
            click.echo("✅ Database schema setup completed")
        except Exception as e:
            click.echo(f"❌ Schema setup failed: {e}")
            return
    
    if verify_schema:
        click.echo("🔍 Verifying PostgreSQL database schema...")
        try:
            is_valid = schema_manager.verify_schema()
            if is_valid:
                click.echo("✅ Schema verification passed")
            else:
                click.echo("❌ Schema verification failed")
        except Exception as e:
            click.echo(f"❌ Schema verification error: {e}")
    
    if demo:
        click.echo("🚀 Running PostgreSQL database integration demo...")
        try:
            demo_runner = DatabaseIntegrationDemo()
            await demo_runner.run_complete_demo()
            click.echo("✅ Integration demo completed successfully")
        except Exception as e:
            click.echo(f"❌ Demo failed: {e}")
    
    if stats:
        click.echo("📊 Getting storage statistics from PostgreSQL...")
        try:
            statistics = persistence_service.get_storage_statistics()
            click.echo(f"Total splits: {statistics.get('total_splits', 0)}")
            click.echo(f"Recent splits (24h): {statistics.get('recent_splits_24h', 0)}")
            click.echo(f"Total games: {statistics.get('total_games', 0)}")
            
            splits_by_source = statistics.get('splits_by_source', {})
            if splits_by_source:
                click.echo("Splits by source:")
                for source, count in splits_by_source.items():
                    click.echo(f"  {source}: {count}")
                    
            splits_by_type = statistics.get('splits_by_type', {})
            if splits_by_type:
                click.echo("Splits by type:")
                for split_type, count in splits_by_type.items():
                    click.echo(f"  {split_type}: {count}")
                    
        except Exception as e:
            click.echo(f"❌ Failed to get statistics: {e}")
    
    if integrity_check:
        click.echo("🔍 Running data integrity check on PostgreSQL...")
        try:
            results = persistence_service.verify_data_integrity()
            click.echo(f"Overall health: {results['overall_health']}")
            click.echo(f"Checks passed: {results['checks_passed']}")
            click.echo(f"Checks failed: {results['checks_failed']}")
            
            if results.get('warnings'):
                click.echo("Warnings:")
                for warning in results['warnings']:
                    click.echo(f"  ⚠️  {warning}")
                    
            if results.get('errors'):
                click.echo("Errors:")
                for error in results['errors']:
                    click.echo(f"  ❌ {error}")
                    
        except Exception as e:
            click.echo(f"❌ Integrity check failed: {e}")
    
    if cleanup is not None:
        click.echo(f"🧹 Cleaning up data older than {cleanup} days from PostgreSQL...")
        try:
            cleanup_stats = persistence_service.cleanup_old_data(days_to_keep=cleanup)
            click.echo(f"Deleted splits: {cleanup_stats['deleted_splits']}")
            click.echo(f"Deleted games: {cleanup_stats['deleted_games']}")
            if cleanup_stats['errors'] > 0:
                click.echo(f"Errors: {cleanup_stats['errors']}")
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
@click.option('--minutes', '-m', type=int, default=60,
              help='Minutes ahead to look for opportunities (default: 60)')
@click.option('--debug', '-d', is_flag=True,
              help='Show all data, regardless of time filters')
@click.option('--format', '-f', 
              type=click.Choice(["console", "json"]),
              default="console",
              help="Output format (default: console)")
@click.option('--output', '-o',
              type=click.Path(path_type=Path),
              help="Output file path for JSON format")
@click.option('--include-cross-market/--no-cross-market', default=True,
              help='Include cross-market flip detection (default: enabled)')
@click.option('--min-flip-confidence', type=float, default=60.0,
              help='Minimum confidence for cross-market flips (default: 60.0)')
def detect_opportunities(minutes: int, debug: bool, format: str, output: Optional[Path], include_cross_market: bool, min_flip_confidence: float):
    """🚨 DEPRECATED: Use 'detect opportunities' instead - Find all betting opportunities using AI-optimized strategies"""
    
    # Show deprecation warning
    click.echo("🚨 DEPRECATION WARNING")
    click.echo("=" * 50)
    click.echo("❌ This command 'detect_opportunities' is DEPRECATED")
    click.echo("✅ Use the new enhanced command instead:")
    click.echo("   mlb-cli detect opportunities --minutes {}".format(minutes))
    click.echo("")
    click.echo("🔄 The new command provides:")
    click.echo("   • Intelligent pipeline orchestration")
    click.echo("   • Better error handling and logging")
    click.echo("   • Enhanced recommendation system")
    click.echo("   • Structured output formats")
    click.echo("")
    click.echo("📚 Available enhanced commands:")
    click.echo("   mlb-cli detect opportunities        # Full detection with pipeline")
    click.echo("   mlb-cli detect smart-pipeline       # Intelligent pipeline execution")
    click.echo("   mlb-cli detect recommendations      # Actual betting recommendations")
    click.echo("   mlb-cli detect system-recommendations   # System maintenance recommendations")
    click.echo("")
    click.echo("⚠️  This deprecated command will be removed in a future version.")
    click.echo("=" * 50)
    
    # Ask user if they want to continue
    if not click.confirm("Do you want to continue with the deprecated command?"):
        click.echo("✅ Use: mlb-cli detect opportunities --minutes {}".format(minutes))
        return
    
    click.echo("⚠️  Running deprecated command...")
    
    async def run_detection():
        from mlb_sharp_betting.db.connection import get_db_manager
        from mlb_sharp_betting.services.cross_market_flip_detector import CrossMarketFlipDetector
        
        # Import the new orchestrator-based detector
        from mlb_sharp_betting.services.adaptive_detector import AdaptiveBettingDetector
        
        db_manager = get_db_manager()
        detector = AdaptiveBettingDetector()
        flip_detector = CrossMarketFlipDetector(db_manager) if include_cross_market else None
        
        try:
            if debug:
                # Run debug mode
                await detector.debug_database_contents()
                return
            
            # Run standard opportunity detection - NEW API
            analysis_result = await detector.analyze_opportunities(minutes)
            
            # Run cross-market flip detection BEFORE display
            cross_market_flips = []
            flip_summary = None
            if include_cross_market and flip_detector:
                hours_back = max(24, minutes // 60 * 4)  # Look back further for flips
                if hours_back <= 24:
                    cross_market_flips, flip_summary = await flip_detector.detect_todays_flips_with_summary(
                        min_confidence=min_flip_confidence
                    )
                else:
                    cross_market_flips = await flip_detector.detect_recent_flips(
                        hours_back=hours_back,
                        min_confidence=min_flip_confidence
                    )
            
            if format == "console":
                # Display analysis using new API
                await detector.display_analysis(analysis_result)
                
                # Always display cross-market flips regardless of debug mode
                if cross_market_flips:
                    click.echo(f"\n🔀 CROSS-MARKET FLIP ANALYSIS")
                    click.echo("=" * 70)
                    click.echo(f"Found {len(cross_market_flips)} cross-market flips with ≥{min_flip_confidence}% confidence")
                    
                    for i, flip in enumerate(cross_market_flips, 1):
                        click.echo(f"\n🎯 FLIP #{i}: {flip.away_team} @ {flip.home_team}")
                        click.echo(f"   📅 Game: {flip.game_datetime.strftime('%Y-%m-%d %H:%M EST')}")
                        click.echo(f"   🔄 Type: {flip.flip_type.value.replace('_', ' ').title()}")
                        click.echo(f"   📊 Confidence: {flip.confidence_score:.1f}%")
                        
                        # Early signal
                        early = flip.early_signal
                        click.echo(f"   📈 Early Signal ({early.hours_before_game:.1f}h before):")
                        click.echo(f"      🎲 {early.split_type.value.title()}: {early.recommended_team}")
                        click.echo(f"      📊 Differential: {early.differential:+.1f}%")
                        click.echo(f"      💪 Strength: {early.strength.value.replace('_', ' ').title()}")
                        click.echo(f"      🏛️  Source: {early.source.value}-{early.book.value if early.book else 'All'}")
                        
                        # Late signal
                        late = flip.late_signal
                        click.echo(f"   📉 Late Signal ({late.hours_before_game:.1f}h before):")
                        click.echo(f"      🎲 {late.split_type.value.title()}: {late.recommended_team}")
                        click.echo(f"      📊 Differential: {late.differential:+.1f}%")
                        click.echo(f"      💪 Strength: {late.strength.value.replace('_', ' ').title()}")
                        
                        # Strategy recommendation
                        click.echo(f"   💡 RECOMMENDATION: {flip.strategy_recommendation}")
                        click.echo(f"   🧠 Reasoning: {flip.reasoning}")
                        
                        # Risk factors
                        if flip.risk_factors:
                            click.echo(f"   ⚠️  Risk Factors:")
                            for risk in flip.risk_factors:
                                click.echo(f"      • {risk}")
                        
                        # Timing gap
                        click.echo(f"   ⏰ Signal Gap: {flip.hours_between_signals:.1f} hours")
                        
                        # Highlight high-confidence flips
                        if flip.confidence_score >= 80:
                            click.echo(f"   🔥 HIGH CONFIDENCE FLIP - STRONG BETTING OPPORTUNITY")
                        elif flip.confidence_score >= 70:
                            click.echo(f"   ✨ GOOD CONFIDENCE FLIP - SOLID BETTING OPPORTUNITY")
                        
                        # ⚠️ CRITICAL WARNING: This strategy is untested
                        click.echo(f"   ⚠️  WARNING: Cross-market flip strategies have NO backtesting results")
                        click.echo(f"   📊 Confidence is theoretical only - strategy performance unknown")
                        click.echo(f"   💡 Use small bet sizes until strategy is proven")
                
                else:
                    if include_cross_market:
                        if flip_summary:
                            click.echo(f"\n🔀 CROSS-MARKET FLIP ANALYSIS")
                            click.echo("=" * 70)
                            click.echo(f"📊 Evaluated {flip_summary['games_evaluated']} games today")
                            click.echo(f"❌ No cross-market flips found with ≥{min_flip_confidence}% confidence")
                        else:
                            click.echo(f"\n🔀 No cross-market flips found with ≥{min_flip_confidence}% confidence")
            
            elif format == "json":
                import json
                from datetime import datetime
                
                # Convert analysis result to JSON-serializable format
                json_games = {}
                for game_key, game_analysis in analysis_result.games.items():
                    away, home, game_time = game_key
                    game_key_str = f"{away}_vs_{home}_{game_time.isoformat()}"
                    json_games[game_key_str] = {
                        'away_team': away,
                        'home_team': home,
                        'game_time': game_time.isoformat(),
                        'sharp_signals': len(game_analysis.sharp_signals),
                        'opposing_markets': len(game_analysis.opposing_markets),
                        'steam_moves': len(game_analysis.steam_moves),
                        'book_conflicts': len(game_analysis.book_conflicts),
                        'total_opportunities': len(game_analysis.sharp_signals) + len(game_analysis.opposing_markets) + len(game_analysis.steam_moves) + len(game_analysis.book_conflicts)
                    }
                
                # Convert cross-market flips to JSON
                json_flips = []
                for flip in cross_market_flips:
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
                            'book': flip.early_signal.book.value if flip.early_signal.book else None
                        },
                        'late_signal': {
                            'split_type': flip.late_signal.split_type.value,
                            'recommended_team': flip.late_signal.recommended_team,
                            'differential': flip.late_signal.differential,
                            'strength': flip.late_signal.strength.value,
                            'hours_before_game': flip.late_signal.hours_before_game,
                            'source': flip.late_signal.source.value,
                            'book': flip.late_signal.book.value if flip.late_signal.book else None
                        },
                        'risk_factors': flip.risk_factors
                    })
                
                json_output = {
                    'timestamp': datetime.now().isoformat(),
                    'analysis_window_minutes': minutes,
                    'total_games': len(analysis_result.games),
                    'total_opportunities': sum(g['total_opportunities'] for g in json_games.values()),
                    'cross_market_flips_count': len(cross_market_flips),
                    'min_flip_confidence': min_flip_confidence,
                    'games': json_games,
                    'cross_market_flips': json_flips,
                    'analysis_metadata': analysis_result.analysis_metadata
                }
                
                json_str = json.dumps(json_output, indent=2)
                if output:
                    output.write_text(json_str)
                    click.echo(f"✅ Analysis saved to: {output}")
                else:
                    click.echo(json_str)
                    
        except Exception as e:
            click.echo(f"❌ Detection failed: {e}")
            sys.exit(1)
        finally:
            # Ensure cleanup
            try:
                if hasattr(detector, 'coordinator') and detector.coordinator:
                    pass  # Coordinator handles cleanup automatically
                if hasattr(detector, 'db_manager') and detector.db_manager:
                    detector.db_manager.close()
                if db_manager:
                    db_manager.close()
            except Exception as cleanup_error:
                click.echo(f"⚠️  Cleanup warning: {cleanup_error}")
    
    click.echo("🎯 ADAPTIVE BETTING DETECTOR")
    click.echo("=" * 50)
    click.echo("🤖 Using orchestrator-powered adaptive strategies")
    if include_cross_market:
        click.echo("🔀 Including cross-market flip detection")
    
    try:
        asyncio.run(run_detection())
    except KeyboardInterrupt:
        click.echo("\n⚠️  Detection interrupted by user")
    except Exception as e:
        click.echo(f"❌ Detection failed: {e}")
        sys.exit(1)


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

# Add individual commands
cli.add_command(orchestrator_demo)

if __name__ == '__main__':
    cli() 