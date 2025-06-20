#!/usr/bin/env python3
"""
MLB Sharp Betting CLI Interface

A user-friendly command-line interface for the MLB Sharp Betting analysis system.
Provides easy access to data scraping, analysis, and reporting functionality.
"""

import asyncio
import sys
from pathlib import Path
from typing import Optional

import click
import structlog

from mlb_sharp_betting.entrypoint import DataPipeline
from mlb_sharp_betting.cli.commands.pre_game import pregame_group
from mlb_sharp_betting.cli.commands.daily_report import daily_report_group
from mlb_sharp_betting.cli.commands.backtesting import backtesting_group

# Configure logging
logger = structlog.get_logger(__name__)


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
            click.echo(f"📁 Database: data/raw/mlb_betting.duckdb")
            
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
    """Query the database"""
    
    from mlb_sharp_betting.db.connection import get_db_manager
    
    click.echo(f"📊 Querying {table} (limit {limit})")
    
    try:
        db_manager = get_db_manager()
        
        with db_manager.get_cursor() as cursor:
            if table == 'splits.raw_mlb_betting_splits':
                cursor.execute(f"""
                    SELECT game_id, home_team, away_team, split_type, 
                           home_or_over_bets_percentage, home_or_over_stake_percentage,
                           sharp_action, last_updated
                    FROM {table}
                    ORDER BY last_updated DESC
                    LIMIT {limit}
                """)
            else:
                cursor.execute(f"SELECT * FROM {table} LIMIT {limit}")
            
            rows = cursor.fetchall()
            columns = [desc[0] for desc in cursor.description]
            
            if not rows:
                click.echo("No data found")
                return
            
            # Print header
            click.echo("\n" + " | ".join(f"{col:15}" for col in columns))
            click.echo("-" * (len(columns) * 17))
            
            # Print rows
            for row in rows:
                click.echo(" | ".join(f"{str(val)[:15]:15}" for val in row))
                
    except Exception as e:
        click.echo(f"❌ Query failed: {e}")


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
            
            if summary[0] == 0:
                click.echo("No data found. Run 'mlb-cli run --mock' first.")
                return
            
            click.echo(f"📊 Total Splits: {summary[0]}")
            click.echo(f"🎯 Sharp Action: {summary[1]} ({summary[1]/summary[0]*100:.1f}%)")
            click.echo(f"📈 Avg Bet/Money Diff: {summary[2]:.1f}%")
            
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
                    click.echo(f"  {game[1]} vs {game[2]} ({game[3]}): {game[6]:.1f}% difference - Sharp: {game[7]}")
            
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
        click.echo("✅ Database: Connected")
        
        with db_manager.get_cursor() as cursor:
            cursor.execute("SELECT COUNT(*) FROM betting_splits")
            count = cursor.fetchone()[0]
            click.echo(f"📊 Betting Splits: {count} records")
            
    except Exception as e:
        click.echo(f"❌ Database: {e}")
    
    # Check data directory
    data_dir = Path("data/raw")
    if data_dir.exists():
        click.echo(f"📁 Data Directory: {data_dir.absolute()}")
        
        db_file = data_dir / "mlb_betting.duckdb"
        if db_file.exists():
            size_mb = db_file.stat().st_size / (1024 * 1024)
            click.echo(f"💾 Database Size: {size_mb:.1f}MB")
    else:
        click.echo("📁 Data Directory: Not found")


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


# Add pre-game commands
cli.add_command(pregame_group)


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
    
    persistence_service = DataPersistenceService()
    schema_manager = SchemaManager()
    
    if setup_schema:
        click.echo("🔧 Setting up database schema...")
        try:
            schema_manager.setup_complete_schema()
            click.echo("✅ Database schema setup completed")
        except Exception as e:
            click.echo(f"❌ Schema setup failed: {e}")
            return
    
    if verify_schema:
        click.echo("🔍 Verifying database schema...")
        try:
            is_valid = schema_manager.verify_schema()
            if is_valid:
                click.echo("✅ Schema verification passed")
            else:
                click.echo("❌ Schema verification failed")
        except Exception as e:
            click.echo(f"❌ Schema verification error: {e}")
    
    if demo:
        click.echo("🚀 Running database integration demo...")
        try:
            demo_runner = DatabaseIntegrationDemo()
            await demo_runner.run_complete_demo()
            click.echo("✅ Integration demo completed successfully")
        except Exception as e:
            click.echo(f"❌ Demo failed: {e}")
    
    if stats:
        click.echo("📊 Getting storage statistics...")
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
        click.echo("🔍 Running data integrity check...")
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
        click.echo(f"🧹 Cleaning up data older than {cleanup} days...")
        try:
            cleanup_stats = persistence_service.cleanup_old_data(days_to_keep=cleanup)
            click.echo(f"Deleted splits: {cleanup_stats['deleted_splits']}")
            click.echo(f"Deleted games: {cleanup_stats['deleted_games']}")
            if cleanup_stats['errors'] > 0:
                click.echo(f"Errors: {cleanup_stats['errors']}")
        except Exception as e:
            click.echo(f"❌ Cleanup failed: {e}")


# Add command groups
cli.add_command(daily_report_group)
cli.add_command(backtesting_group)


if __name__ == '__main__':
    cli() 