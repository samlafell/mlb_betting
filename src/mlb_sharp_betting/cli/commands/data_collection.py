"""
CLI commands for data collection and freshness management.
"""

import click
import asyncio
from datetime import datetime, timedelta
from typing import Dict, Any, Optional
import structlog

from ...entrypoint import DataPipeline
from ...services.backtesting_engine import get_backtesting_engine
from ...services.database_coordinator import get_database_coordinator
from ...utils.time_based_validator import get_game_time_validator
from ...db.connection import get_db_manager
from ...services.data_service import get_data_service
from ...core.logging import get_logger

logger = structlog.get_logger(__name__)


async def check_data_freshness(db_manager) -> Dict[str, Any]:
    """Simple data freshness check using direct database queries."""
    try:
        with db_manager.get_cursor() as cursor:
            # Get latest splits timestamp
            cursor.execute("""
                SELECT 
                    MAX(last_updated) as latest_update,
                    COUNT(*) as total_splits,
                    COUNT(DISTINCT CONCAT(home_team, '|', away_team)) as unique_games
                FROM splits.raw_mlb_betting_splits
            """)
            splits_info = cursor.fetchone()
            
            # Get latest outcomes timestamp
            cursor.execute("""
                SELECT MAX(created_at) as latest_outcome, COUNT(*) as total_outcomes
                FROM public.game_outcomes
            """)
            outcomes_info = cursor.fetchone() if cursor.rowcount > 0 else {'latest_outcome': None, 'total_outcomes': 0}
            
            # Calculate data age
            latest_update = splits_info['latest_update'] if splits_info else None
            data_age_hours = 0
            if latest_update:
                data_age_hours = (datetime.now() - latest_update).total_seconds() / 3600
            
            # Data is considered fresh if less than 6 hours old
            max_age_hours = 6
            is_fresh = data_age_hours < max_age_hours
            
            return {
                'is_fresh': is_fresh,
                'data_age_hours': data_age_hours,
                'max_age_hours': max_age_hours,
                'latest_splits_update': latest_update,
                'latest_outcomes_update': outcomes_info.get('latest_outcome'),
                'total_splits': splits_info.get('total_splits', 0) if splits_info else 0,
                'unique_games': splits_info.get('unique_games', 0) if splits_info else 0,
                'total_outcomes': outcomes_info.get('total_outcomes', 0),
                'needs_collection': not is_fresh
            }
    except Exception as e:
        logger.error("Data freshness check failed", error=str(e))
        return {
            'is_fresh': False,
            'error': str(e),
            'data_age_hours': 999,  # Very old
            'needs_collection': True
        }


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
                db_manager = get_db_manager()
                freshness_check = await check_data_freshness(db_manager)
                
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
                db_manager = get_db_manager()
                freshness_check = await check_data_freshness(db_manager)
                
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
            
            # Display timing validation results
            storage_stats = metrics.get('storage_stats', {})
            if storage_stats.get('timing_rejections', 0) > 0:
                timing_rejections = storage_stats['timing_rejections']
                total_processed = storage_stats.get('processed', 0) + timing_rejections
                rejection_rate = (timing_rejections / total_processed * 100) if total_processed > 0 else 0
                click.echo(f"   ⏰ Timing Rejections: {timing_rejections} ({rejection_rate:.1f}%)")
                
                # Show alert if rejection rate is high
                if rejection_rate > 20:
                    click.echo(f"   ⚠️  HIGH REJECTION RATE - Most games may have already started")
                
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
            db_manager = get_db_manager()
            freshness_check = await check_data_freshness(db_manager)
            
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
            data_service = get_data_service(get_db_manager())
            
            if dry_run:
                # Show what would be deleted
                with get_db_manager().get_cursor() as cursor:
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
                cleanup_stats = data_service.persistence.cleanup_old_data(days_to_keep=days)
                
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


@data_collection_group.command('timing-status')
@click.option('--detailed', is_flag=True, help='Show detailed timing validation metrics')
@click.option('--check-expired', is_flag=True, help='Check for recently expired splits')
def timing_validation_status(detailed: bool, check_expired: bool):
    """⏰ Check timing validation status and 5-minute grace period metrics"""
    
    click.echo("⏰ TIMING VALIDATION STATUS")
    click.echo("=" * 50)
    
    try:
        # Get database coordinator for timing queries
        coordinator = get_database_coordinator()
        
        # Get timing validation metrics
        timing_status = coordinator.get_timing_validation_status()
        
        if timing_status['status'] == 'error':
            click.echo(f"❌ Error getting timing status: {timing_status.get('error')}")
            return
        
        if timing_status['status'] == 'no_data':
            click.echo("📭 No recent data found for timing validation")
            return
        
        # Display daily metrics
        daily_metrics = timing_status.get('daily_metrics', [])
        if daily_metrics:
            click.echo("📊 Recent Daily Metrics (7 days):")
            click.echo(f"{'Date':<12} {'Total':<8} {'Valid':<8} {'Expired':<8} {'Rejection %':<12}")
            click.echo("-" * 50)
            
            for day in daily_metrics[:7]:
                if isinstance(day, dict):
                    date = day.get('game_date', 'Unknown')
                    total = day.get('total_splits', 0)
                    valid = day.get('valid_splits', 0)
                    expired = day.get('expired_splits', 0)
                    rejection_rate = day.get('rejection_rate_percent', 0)
                else:
                    # Handle tuple/list format
                    date = day[0] if len(day) > 0 else 'Unknown'
                    total = day[1] if len(day) > 1 else 0
                    valid = day[2] if len(day) > 2 else 0
                    expired = day[3] if len(day) > 3 else 0
                    rejection_rate = day[4] if len(day) > 4 else 0
                
                # Color code rejection rate
                if rejection_rate > 20:
                    rate_display = f"🔴 {rejection_rate:.1f}%"
                elif rejection_rate > 10:
                    rate_display = f"🟡 {rejection_rate:.1f}%"
                else:
                    rate_display = f"🟢 {rejection_rate:.1f}%"
                
                click.echo(f"{str(date):<12} {total:<8} {valid:<8} {expired:<8} {rate_display:<12}")
        
        # Get validator stats
        try:
            validator = get_game_time_validator()
            validator_stats = validator.get_validation_stats()
            
            click.echo(f"\n🔍 Validator Statistics:")
            click.echo(f"   Total Validations: {validator_stats.get('total_validations', 0):,}")
            click.echo(f"   Valid Splits: {validator_stats.get('valid_splits', 0):,}")
            click.echo(f"   Rejected Splits: {validator_stats.get('expired_splits', 0):,}")
            click.echo(f"   Delayed Games: {validator_stats.get('delayed_games', 0):,}")
            click.echo(f"   Postponed Games: {validator_stats.get('postponed_games', 0):,}")
            
            # Check for alerts
            should_alert, alert_reasons = validator.should_alert()
            if should_alert:
                click.echo(f"\n⚠️  VALIDATION ALERTS:")
                for reason in alert_reasons:
                    click.echo(f"   • {reason}")
            else:
                click.echo(f"\n✅ No validation alerts")
                
        except Exception as e:
            click.echo(f"\n⚠️  Could not get validator stats: {e}")
        
        # Check for recently expired splits if requested
        if check_expired:
            click.echo(f"\n🕐 Checking Recently Expired Splits...")
            expired_check = coordinator.check_expired_splits(hours_back=24)
            
            expired_splits = expired_check.get('expired_splits', [])
            if expired_splits:
                click.echo(f"\n❌ Found {len(expired_splits)} splits stored after games started:")
                click.echo(f"{'Game ID':<15} {'Teams':<25} {'Minutes Late':<12} {'Game Time'}")
                click.echo("-" * 70)
                
                for split in expired_splits[:10]:  # Show top 10
                    if isinstance(split, dict):
                        game_id = split.get('game_id', 'Unknown')[:14]
                        teams = f"{split.get('away_team', '?')} @ {split.get('home_team', '?')}"[:24]
                        minutes_late = split.get('minutes_after_start', 0)
                        game_time = str(split.get('game_datetime', 'Unknown'))[:16]
                    else:
                        game_id = str(split[0])[:14] if len(split) > 0 else 'Unknown'
                        teams = f"{split[2]} @ {split[1]}"[:24] if len(split) > 2 else "Unknown"
                        minutes_late = split[5] if len(split) > 5 else 0
                        game_time = str(split[3])[:16] if len(split) > 3 else 'Unknown'
                    
                    click.echo(f"{game_id:<15} {teams:<25} {minutes_late:>8.1f}    {game_time}")
                    
                if len(expired_splits) > 10:
                    click.echo(f"   ... and {len(expired_splits) - 10} more")
            else:
                click.echo(f"\n✅ No expired splits found in last 24 hours")
        
        # Show current games status if detailed
        if detailed:
            click.echo(f"\n🎮 Current Games Status:")
            games_status = coordinator.get_current_games_status()
            
            games = games_status.get('games', [])
            status_breakdown = games_status.get('status_breakdown', {})
            
            if status_breakdown:
                click.echo(f"   📈 Status Breakdown:")
                for status, count in status_breakdown.items():
                    click.echo(f"      {status}: {count} games")
            
            if games:
                click.echo(f"\n   📋 Upcoming Games (next 18 hours):")
                for game in games[:5]:  # Show first 5
                    if isinstance(game, dict):
                        teams = f"{game.get('away_team', '?')} @ {game.get('home_team', '?')}"
                        timing_status = game.get('timing_status', 'unknown')
                        game_time = str(game.get('game_datetime', 'Unknown'))[:16]
                        minutes = game.get('minutes_since_start', 0)
                    else:
                        teams = f"{game[2]} @ {game[1]}" if len(game) > 2 else "Unknown"
                        timing_status = game[5] if len(game) > 5 else 'unknown'
                        game_time = str(game[3])[:16] if len(game) > 3 else 'Unknown'
                        minutes = game[6] if len(game) > 6 else 0
                    
                    status_icon = {
                        'future_game': '🔮',
                        'within_grace_period': '✅',
                        'expired': '❌',
                        'invalid_time': '⚠️'
                    }.get(timing_status, '❓')
                    
                    click.echo(f"      {status_icon} {teams:<25} {game_time} ({timing_status})")
        
        click.echo(f"\n💡 Tip: Use --detailed for more information, --check-expired to see violations")
        
    except Exception as e:
        click.echo(f"❌ Timing validation check failed: {e}")
        raise