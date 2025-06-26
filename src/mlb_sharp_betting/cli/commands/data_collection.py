"""
CLI commands for data collection and freshness management.
"""

import click
import asyncio
from datetime import datetime
from typing import Optional
import structlog

from ...entrypoint import DataPipeline
from ...services.enhanced_backtesting_service import EnhancedBacktestingService
from ...db.connection import get_db_manager

logger = structlog.get_logger(__name__)


@click.group()
def data_collection_group():
    """📡 Data collection and freshness management commands."""
    pass


@data_collection_group.command('collect')
@click.option('--sport', default='mlb', help='Sport to collect data for')
@click.option('--sportsbook', default='circa', help='Sportsbook to use')
@click.option('--dry-run', is_flag=True, help='Use mock data instead of scraping')
@click.option('--validate-only', is_flag=True, help='Only validate existing data freshness')
@click.option('--force', is_flag=True, help='Force collection even if data is fresh')
def collect_data(sport: str, sportsbook: str, dry_run: bool, validate_only: bool, force: bool):
    """📡 Collect fresh betting data from all sources"""
    
    async def run_collection():
        click.echo("📡 DATA COLLECTION SERVICE")
        click.echo("=" * 50)
        
        if validate_only:
            click.echo("🔍 Validating existing data freshness...")
            
            try:
                enhanced_service = EnhancedBacktestingService()
                freshness_check = await enhanced_service.check_data_freshness()
                
                click.echo(f"\n📊 Data Freshness Report:")
                click.echo(f"   📅 Latest Update: {freshness_check.get('latest_splits_update', 'Unknown')}")
                click.echo(f"   ⏰ Data Age: {freshness_check.get('data_age_hours', 0):.1f} hours")
                click.echo(f"   📈 Total Splits: {freshness_check.get('total_splits', 0):,}")
                click.echo(f"   🎮 Unique Games: {freshness_check.get('unique_games', 0)}")
                click.echo(f"   🏆 Game Outcomes: {freshness_check.get('total_outcomes', 0)}")
                
                if freshness_check['is_fresh']:
                    click.echo(f"\n✅ Data is fresh (within {freshness_check['max_age_hours']} hours)")
                    click.echo("💡 No collection needed")
                else:
                    click.echo(f"\n⚠️  Data is stale (older than {freshness_check['max_age_hours']} hours)")
                    click.echo("💡 Collection recommended")
                    
                return
                
            except Exception as e:
                click.echo(f"❌ Validation failed: {e}")
                return
        
        # Check freshness before collection (unless forced)
        if not force:
            try:
                enhanced_service = EnhancedBacktestingService()
                freshness_check = await enhanced_service.check_data_freshness()
                
                if freshness_check['is_fresh']:
                    click.echo(f"✅ Data is already fresh ({freshness_check['data_age_hours']:.1f} hours old)")
                    click.echo("💡 Use --force to collect anyway")
                    return
                    
            except Exception as e:
                click.echo(f"⚠️  Could not check freshness: {e}")
                click.echo("🔄 Proceeding with collection...")
        
        # Run data collection
        click.echo(f"🚀 Starting data collection...")
        click.echo(f"   🏈 Sport: {sport}")
        click.echo(f"   🏛️  Sportsbook: {sportsbook}")
        click.echo(f"   🧪 Mode: {'Mock Data' if dry_run else 'Live Scraping'}")
        
        try:
            data_pipeline = DataPipeline(
                sport=sport,
                sportsbook=sportsbook,
                dry_run=dry_run
            )
            
            metrics = await data_pipeline.run()
            
            # Display results
            click.echo(f"\n✅ DATA COLLECTION COMPLETED")
            click.echo(f"   📥 Records Scraped: {metrics.get('scraped_records', 0)}")
            click.echo(f"   🔄 Records Parsed: {metrics.get('parsed_records', 0)}")
            click.echo(f"   💾 Records Stored: {metrics.get('stored_records', 0)}")
            click.echo(f"   🎯 Sharp Indicators: {metrics.get('sharp_indicators', 0)}")
            
            if metrics.get('errors', 0) > 0:
                click.echo(f"   ❌ Errors: {metrics['errors']}")
            
            duration = (metrics['end_time'] - metrics['start_time']).total_seconds()
            click.echo(f"   ⏱️  Duration: {duration:.2f}s")
            
            # Show database info
            click.echo(f"\n📁 Database: PostgreSQL (mlb_betting)")
            
        except Exception as e:
            click.echo(f"❌ Data collection failed: {e}")
            raise
    
    try:
        asyncio.run(run_collection())
    except KeyboardInterrupt:
        click.echo("\n⚠️  Collection interrupted by user")
    except Exception:
        click.echo("❌ Collection failed")
        raise


@data_collection_group.command('status')
@click.option('--detailed', is_flag=True, help='Show detailed data breakdown')
def data_status(detailed: bool):
    """📊 Check data freshness and quality"""
    
    async def check_status():
        click.echo("📊 DATA STATUS REPORT")
        click.echo("=" * 50)
        
        try:
            enhanced_service = EnhancedBacktestingService()
            freshness_check = await enhanced_service.check_data_freshness()
            
            # Basic status
            click.echo(f"🕐 Data Age: {freshness_check.get('data_age_hours', 0):.1f} hours")
            
            if freshness_check['is_fresh']:
                click.echo(f"✅ Status: FRESH (within {freshness_check['max_age_hours']} hours)")
            else:
                click.echo(f"⚠️  Status: STALE (older than {freshness_check['max_age_hours']} hours)")
            
            # Data metrics
            click.echo(f"\n📈 Data Metrics:")
            click.echo(f"   📊 Betting Splits: {freshness_check.get('total_splits', 0):,}")
            click.echo(f"   🎮 Unique Games: {freshness_check.get('unique_games', 0)}")
            click.echo(f"   🏆 Game Outcomes: {freshness_check.get('total_outcomes', 0)}")
            
            # Timestamps
            click.echo(f"\n📅 Last Updates:")
            splits_update = freshness_check.get('latest_splits_update')
            outcomes_update = freshness_check.get('latest_outcomes_update')
            
            click.echo(f"   📊 Splits: {splits_update.strftime('%Y-%m-%d %H:%M:%S') if splits_update else 'None'}")
            click.echo(f"   🏆 Outcomes: {outcomes_update.strftime('%Y-%m-%d %H:%M:%S') if outcomes_update else 'None'}")
            
            if detailed:
                # Show detailed breakdown
                db_manager = get_db_manager()
                try:
                    with db_manager.get_cursor() as cursor:
                        # Splits by source
                        cursor.execute("""
                            SELECT source, COUNT(*) as count
                            FROM splits.raw_mlb_betting_splits
                            WHERE last_updated >= CURRENT_DATE - INTERVAL '7 days'
                            GROUP BY source
                            ORDER BY count DESC
                        """)
                        splits_by_source = cursor.fetchall()
                        
                        if splits_by_source:
                            click.echo(f"\n📡 Recent Splits by Source (7 days):")
                            for row in splits_by_source:
                                click.echo(f"   {row['source']}: {row['count']:,}")
                        
                        # Splits by type
                        cursor.execute("""
                            SELECT split_type, COUNT(*) as count
                            FROM splits.raw_mlb_betting_splits
                            WHERE last_updated >= CURRENT_DATE - INTERVAL '7 days'
                            GROUP BY split_type
                            ORDER BY count DESC
                        """)
                        splits_by_type = cursor.fetchall()
                        
                        if splits_by_type:
                            click.echo(f"\n🎲 Recent Splits by Type (7 days):")
                            for row in splits_by_type:
                                click.echo(f"   {row['split_type']}: {row['count']:,}")
                                
                finally:
                    db_manager.close()
            
            # Recommendations
            if freshness_check.get('needs_collection'):
                click.echo(f"\n💡 Recommendation: Run 'mlb-cli data collect' to refresh data")
            else:
                click.echo(f"\n✨ Data is current - ready for analysis")
                
        except Exception as e:
            click.echo(f"❌ Status check failed: {e}")
    
    try:
        asyncio.run(check_status())
    except Exception:
        click.echo("❌ Status check failed")
        raise


@data_collection_group.command('cleanup')
@click.option('--days', type=int, default=30, help='Keep data newer than N days')
@click.option('--dry-run', is_flag=True, help='Show what would be deleted without deleting')
@click.confirmation_option(prompt='Are you sure you want to clean up old data?')
def cleanup_data(days: int, dry_run: bool):
    """🧹 Clean up old data from the database"""
    
    async def run_cleanup():
        click.echo("🧹 DATA CLEANUP SERVICE")
        click.echo("=" * 50)
        click.echo(f"🗑️  Removing data older than {days} days")
        
        if dry_run:
            click.echo("🧪 DRY RUN MODE - No data will be deleted")
        
        try:
            from ...services.data_persistence import DataPersistenceService
            
            db_manager = get_db_manager()
            persistence_service = DataPersistenceService(db_manager)
            
            if dry_run:
                # Show what would be deleted
                with db_manager.get_cursor() as cursor:
                    cursor.execute("""
                        SELECT COUNT(*) as old_splits
                        FROM splits.raw_mlb_betting_splits
                        WHERE last_updated < CURRENT_DATE - INTERVAL '%s days'
                    """, (days,))
                    old_splits = cursor.fetchone()['old_splits']
                    
                    cursor.execute("""
                        SELECT COUNT(*) as old_games
                        FROM main.games
                        WHERE created_at < CURRENT_DATE - INTERVAL '%s days'
                    """, (days,))
                    old_games = cursor.fetchone()['old_games']
                    
                    click.echo(f"\n📊 Would delete:")
                    click.echo(f"   📊 Betting splits: {old_splits:,}")
                    click.echo(f"   🎮 Games: {old_games:,}")
                    click.echo(f"\n💡 Run without --dry-run to execute cleanup")
            else:
                # Actually clean up
                cleanup_stats = persistence_service.cleanup_old_data(days_to_keep=days)
                
                click.echo(f"\n✅ CLEANUP COMPLETED")
                click.echo(f"   📊 Deleted splits: {cleanup_stats['deleted_splits']:,}")
                click.echo(f"   🎮 Deleted games: {cleanup_stats['deleted_games']:,}")
                
                if cleanup_stats['errors'] > 0:
                    click.echo(f"   ❌ Errors: {cleanup_stats['errors']}")
                
                click.echo(f"\n💾 Database space freed")
            
        except Exception as e:
            click.echo(f"❌ Cleanup failed: {e}")
            raise
    
    try:
        asyncio.run(run_cleanup())
    except Exception:
        click.echo("❌ Cleanup failed")
        raise 