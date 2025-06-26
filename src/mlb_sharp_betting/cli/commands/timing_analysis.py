"""
CLI commands for betting recommendation timing analysis.

This module provides commands to analyze timing performance and get real-time
timing recommendations for optimal bet placement.
"""

import asyncio
import sys
from datetime import datetime, timedelta, timezone
from typing import Optional
from pathlib import Path

import click
import structlog
from tabulate import tabulate

from mlb_sharp_betting.db.connection import get_db_manager
from mlb_sharp_betting.services.timing_analysis_service import TimingAnalysisService
from mlb_sharp_betting.models.timing_analysis import RealtimeTimingLookup
from mlb_sharp_betting.models.splits import SplitType, DataSource, BookType

logger = structlog.get_logger(__name__)


@click.group()
def timing_group():
    """Betting recommendation timing analysis commands."""
    pass


@timing_group.command()
@click.option('--start-date', '-s',
              type=click.DateTime(formats=['%Y-%m-%d']),
              help='Start date for analysis (YYYY-MM-DD)')
@click.option('--end-date', '-e', 
              type=click.DateTime(formats=['%Y-%m-%d']),
              help='End date for analysis (YYYY-MM-DD)')
@click.option('--days-back', '-d',
              type=int,
              default=30,
              help='Number of days to look back (if no dates specified)')
@click.option('--source',
              type=click.Choice(['VSIN', 'SBD']),
              help='Filter by data source')
@click.option('--book',
              type=click.Choice(['circa', 'draftkings', 'fanduel', 'betmgm']),
              help='Filter by sportsbook')
@click.option('--split-type',
              type=click.Choice(['moneyline', 'spread', 'total']),
              help='Filter by bet type')
@click.option('--strategy',
              type=str,
              help='Filter by strategy name')
@click.option('--min-sample-size',
              type=int,
              default=10,
              help='Minimum bets required for bucket analysis')
@click.option('--format', '-f',
              type=click.Choice(['console', 'json', 'csv']),
              default='console',
              help='Output format')
@click.option('--output', '-o',
              type=click.Path(path_type=Path),
              help='Output file path for JSON/CSV formats')
def analyze(
    start_date: Optional[datetime],
    end_date: Optional[datetime], 
    days_back: int,
    source: Optional[str],
    book: Optional[str],
    split_type: Optional[str],
    strategy: Optional[str],
    min_sample_size: int,
    format: str,
    output: Optional[Path]
):
    """Analyze betting recommendation timing performance."""
    
    # Set date range
    if not start_date or not end_date:
        end_date = datetime.now(timezone.utc)
        start_date = end_date - timedelta(days=days_back)
    else:
        # Ensure timezone awareness
        if start_date.tzinfo is None:
            start_date = start_date.replace(tzinfo=timezone.utc)
        if end_date.tzinfo is None:
            end_date = end_date.replace(tzinfo=timezone.utc)
    
    async def run_analysis():
        click.echo(f"🕐 TIMING ANALYSIS")
        click.echo(f"📅 Period: {start_date.date()} to {end_date.date()}")
        click.echo(f"🔍 Filters: Source={source or 'All'}, Book={book or 'All'}, Type={split_type or 'All'}, Strategy={strategy or 'All'}")
        click.echo(f"📊 Min Sample Size: {min_sample_size} bets")
        click.echo("=" * 80)
        
        # Convert string enums to proper types
        source_enum = DataSource(source) if source else None
        book_enum = BookType(book) if book else None
        split_type_enum = SplitType(split_type) if split_type else None
        
        # Setup service
        db_manager = get_db_manager()
        timing_service = TimingAnalysisService(db_manager)
        
        try:
            # Run analysis
            analysis = await timing_service.analyze_timing_performance(
                start_date=start_date,
                end_date=end_date,
                source=source_enum,
                book=book_enum,
                split_type=split_type_enum,
                strategy_name=strategy,
                minimum_sample_size=min_sample_size
            )
            
            if format == 'console':
                _display_console_analysis(analysis)
            elif format == 'json':
                _export_json_analysis(analysis, output)
            elif format == 'csv':
                _export_csv_analysis(analysis, output)
                
        except Exception as e:
            click.echo(f"❌ Analysis failed: {e}", err=True)
            sys.exit(1)
        finally:
            db_manager.close()
    
    asyncio.run(run_analysis())


@timing_group.command()
@click.option('--hours-until-game', '-h',
              type=float,
              required=True,
              help='Hours until game start')
@click.option('--source',
              type=click.Choice(['VSIN', 'SBD']),
              help='Data source for the recommendation')
@click.option('--book',
              type=click.Choice(['circa', 'draftkings', 'fanduel', 'betmgm']),
              help='Sportsbook for the recommendation')
@click.option('--split-type',
              type=click.Choice(['moneyline', 'spread', 'total']),
              required=True,
              help='Type of bet being considered')
@click.option('--strategy',
              type=str,
              help='Strategy generating the recommendation')
@click.option('--no-cache',
              is_flag=True,
              help='Skip cache and generate fresh recommendation')
def recommend(
    hours_until_game: float,
    source: Optional[str],
    book: Optional[str],
    split_type: str,
    strategy: Optional[str],
    no_cache: bool
):
    """Get real-time timing recommendation for a betting opportunity."""
    
    async def get_recommendation():
        click.echo(f"🎯 TIMING RECOMMENDATION")
        click.echo(f"⏰ Hours until game: {hours_until_game}")
        click.echo(f"🎲 Bet type: {split_type}")
        click.echo(f"📡 Source: {source or 'Any'}")
        click.echo(f"🏛️  Book: {book or 'Any'}")
        click.echo(f"🧠 Strategy: {strategy or 'Any'}")
        click.echo("=" * 60)
        
        # Convert to proper types
        source_enum = DataSource(source) if source else None
        book_enum = BookType(book) if book else None
        split_type_enum = SplitType(split_type)
        
        # Create lookup
        lookup = RealtimeTimingLookup(
            hours_until_game=hours_until_game,
            source=source_enum,
            book=book_enum,
            split_type=split_type_enum,
            strategy_name=strategy
        )
        
        # Setup service
        db_manager = get_db_manager()
        timing_service = TimingAnalysisService(db_manager)
        
        try:
            # Get recommendation
            recommendation = await timing_service.get_realtime_timing_recommendation(
                lookup=lookup,
                use_cache=not no_cache
            )
            
            _display_recommendation(recommendation)
            
        except Exception as e:
            click.echo(f"❌ Recommendation failed: {e}", err=True)
            sys.exit(1)
        finally:
            db_manager.close()
    
    asyncio.run(get_recommendation())


@timing_group.command()
@click.option('--days-back', '-d',
              type=int,
              default=30,
              help='Number of days to look back')
@click.option('--min-sample-size',
              type=int,
              default=10,
              help='Minimum bets for inclusion')
def summary(days_back: int, min_sample_size: int):
    """Show timing performance summary."""
    
    async def show_summary():
        click.echo(f"📊 TIMING PERFORMANCE SUMMARY")
        click.echo(f"📅 Last {days_back} days")
        click.echo(f"📈 Minimum {min_sample_size} bets")
        click.echo("=" * 70)
        
        # Setup service
        db_manager = get_db_manager()
        timing_service = TimingAnalysisService(db_manager)
        
        try:
            # Get summary
            summary_data = await timing_service.get_timing_performance_summary(
                days_back=days_back,
                minimum_sample_size=min_sample_size
            )
            
            if not summary_data:
                click.echo("❌ No timing data found for the specified period")
                return
            
            _display_summary_table(summary_data)
            
        except Exception as e:
            click.echo(f"❌ Summary failed: {e}", err=True)
            sys.exit(1)
        finally:
            db_manager.close()
    
    asyncio.run(show_summary())


@timing_group.command()
def update_outcomes():
    """Update recommendation outcomes using game results."""
    
    async def update():
        click.echo("🔄 Updating recommendation outcomes...")
        
        # Setup service
        db_manager = get_db_manager()
        timing_service = TimingAnalysisService(db_manager)
        
        try:
            updated_count = await timing_service.update_recommendation_outcomes()
            click.echo(f"✅ Updated {updated_count} recommendation outcomes")
            
        except Exception as e:
            click.echo(f"❌ Update failed: {e}", err=True)
            sys.exit(1)
        finally:
            db_manager.close()
    
    asyncio.run(update())


@timing_group.command()
@click.option('--game-id', required=True, help='Game ID')
@click.option('--home-team', required=True, help='Home team abbreviation')
@click.option('--away-team', required=True, help='Away team abbreviation')
@click.option('--game-datetime', required=True, 
              type=click.DateTime(formats=['%Y-%m-%d %H:%M']),
              help='Game date and time (YYYY-MM-DD HH:MM)')
@click.option('--source', required=True,
              type=click.Choice(['VSIN', 'SBD']),
              help='Data source')
@click.option('--book',
              type=click.Choice(['circa', 'draftkings', 'fanduel', 'betmgm']),
              help='Sportsbook')
@click.option('--split-type', required=True,
              type=click.Choice(['moneyline', 'spread', 'total']),
              help='Bet type')
@click.option('--strategy', required=True, help='Strategy name')
@click.option('--recommended-side', required=True, help='Recommended betting side')
@click.option('--odds', type=float, help='Odds at recommendation time')
@click.option('--units', type=float, default=1.0, help='Units wagered')
def track(
    game_id: str,
    home_team: str,
    away_team: str,
    game_datetime: datetime,
    source: str,
    book: Optional[str],
    split_type: str,
    strategy: str,
    recommended_side: str,
    odds: Optional[float],
    units: float
):
    """Track a betting recommendation for timing analysis."""
    
    async def track_recommendation():
        click.echo(f"📝 Tracking recommendation...")
        click.echo(f"🎮 Game: {away_team} @ {home_team}")
        click.echo(f"📅 Game time: {game_datetime}")
        click.echo(f"💡 Strategy: {strategy}")
        click.echo(f"🎯 Side: {recommended_side}")
        
        # Convert to proper types
        source_enum = DataSource(source)
        book_enum = BookType(book) if book else None
        split_type_enum = SplitType(split_type)
        
        # Ensure timezone awareness
        if game_datetime.tzinfo is None:
            game_datetime_tz = game_datetime.replace(tzinfo=timezone.utc)
        else:
            game_datetime_tz = game_datetime
        
        # Setup service
        db_manager = get_db_manager()
        timing_service = TimingAnalysisService(db_manager)
        
        try:
            success = await timing_service.track_recommendation(
                game_id=game_id,
                home_team=home_team,
                away_team=away_team,
                game_datetime=game_datetime_tz,
                source=source_enum,
                book=book_enum,
                split_type=split_type_enum,
                strategy_name=strategy,
                recommended_side=recommended_side,
                odds_at_recommendation=odds,
                units_wagered=units
            )
            
            if success:
                click.echo("✅ Recommendation tracked successfully")
            else:
                click.echo("❌ Failed to track recommendation")
                sys.exit(1)
                
        except Exception as e:
            click.echo(f"❌ Tracking failed: {e}", err=True)
            sys.exit(1)
        finally:
            db_manager.close()
    
    asyncio.run(track_recommendation())


def _display_console_analysis(analysis):
    """Display comprehensive timing analysis in console format."""
    
    click.echo(f"\n📊 {analysis.analysis_name}")
    click.echo(f"🎮 Games analyzed: {analysis.total_games_analyzed}")
    click.echo(f"📋 Total recommendations: {analysis.total_recommendations}")
    
    # Overall performance
    overall = analysis.overall_metrics
    if overall.total_bets > 0:
        click.echo(f"\n🎯 OVERALL PERFORMANCE")
        click.echo(f"   Total bets: {overall.total_bets}")
        click.echo(f"   Win rate: {overall.win_rate:.1f}%")
        click.echo(f"   ROI: {overall.roi_percentage:.1f}%")
        click.echo(f"   Profit/Loss: {overall.total_profit_loss:+.2f} units")
        click.echo(f"   Confidence: {overall.confidence_level.value}")
        
        if overall.odds_movement:
            click.echo(f"   Avg odds movement: {overall.odds_movement:+.1f}")
    
    # Bucket performance
    if analysis.bucket_analyses:
        click.echo(f"\n📈 TIMING BUCKET PERFORMANCE")
        
        table_data = []
        # Sort buckets by predefined order
        bucket_order = {'0-2h': 0, '2-6h': 1, '6-24h': 2, '24h+': 3}
        
        for bucket_analysis in sorted(analysis.bucket_analyses, 
                                    key=lambda x: bucket_order.get(
                                        x.timing_bucket.value if hasattr(x.timing_bucket, 'value') else x.timing_bucket, 
                                        999
                                    )):
            metrics = bucket_analysis.metrics
            bucket_name = bucket_analysis.timing_bucket.display_name if hasattr(bucket_analysis.timing_bucket, 'display_name') else bucket_analysis.timing_bucket
            table_data.append([
                bucket_name,
                metrics.total_bets,
                f"{metrics.win_rate:.1f}%",
                f"{metrics.roi_percentage:.1f}%",
                f"{metrics.total_profit_loss:+.2f}",
                metrics.confidence_level.value,
                bucket_analysis.performance_grade
            ])
        
        headers = ["Timing", "Bets", "Win Rate", "ROI", "Profit/Loss", "Confidence", "Grade"]
        click.echo(tabulate(table_data, headers=headers, tablefmt="grid"))
    
    # Best performers
    if analysis.best_bucket:
        click.echo(f"\n🏆 BEST PERFORMERS")
        best_bucket_name = analysis.best_bucket.display_name if hasattr(analysis.best_bucket, 'display_name') else analysis.best_bucket
        click.echo(f"   Best timing: {best_bucket_name}")
        if analysis.best_source:
            best_source_name = analysis.best_source.value if hasattr(analysis.best_source, 'value') else analysis.best_source
            click.echo(f"   Best source: {best_source_name}")
        if analysis.best_strategy:
            click.echo(f"   Best strategy: {analysis.best_strategy}")
    
    # Optimal timing recommendation
    optimal_rec = analysis.optimal_timing_recommendation
    if optimal_rec != "INSUFFICIENT_DATA":
        click.echo(f"\n💡 {optimal_rec}")
    
    # Trends
    if analysis.trends:
        click.echo(f"\n📊 TREND ANALYSIS")
        for key, value in analysis.trends.items():
            formatted_key = key.replace('_', ' ').title()
            click.echo(f"   {formatted_key}: {value}")


def _display_recommendation(recommendation):
    """Display timing recommendation in console format."""
    
    lookup = recommendation.lookup
    
    bucket_name = lookup.timing_bucket.display_name if hasattr(lookup.timing_bucket, 'display_name') else lookup.timing_bucket
    click.echo(f"\n⏰ TIMING BUCKET: {bucket_name}")
    click.echo(f"🎯 CONFIDENCE: {recommendation.confidence}")
    click.echo(f"📋 ACTION: {recommendation.action_needed}")
    
    if recommendation.sample_size_warning:
        click.echo(f"⚠️  WARNING: Insufficient sample size for reliable prediction")
    
    click.echo(f"\n💬 RECOMMENDATION:")
    click.echo(f"   {recommendation.recommendation}")
    
    if recommendation.expected_win_rate is not None:
        click.echo(f"\n📊 EXPECTED PERFORMANCE:")
        click.echo(f"   Win rate: {recommendation.expected_win_rate:.1f}%")
        if recommendation.expected_roi is not None:
            click.echo(f"   ROI: {recommendation.expected_roi:.1f}%")
    
    if recommendation.historical_metrics:
        hist = recommendation.historical_metrics
        click.echo(f"\n📈 HISTORICAL DATA:")
        click.echo(f"   Sample size: {hist.total_bets} bets")
        click.echo(f"   Win rate: {hist.win_rate:.1f}%")
        click.echo(f"   ROI: {hist.roi_percentage:.1f}%")
        click.echo(f"   Confidence: {hist.confidence_level.value}")
    
    if recommendation.risk_factors:
        click.echo(f"\n⚠️  RISK FACTORS:")
        for factor in recommendation.risk_factors:
            click.echo(f"   • {factor}")
    
    # Color coding for action
    if recommendation.is_recommended:
        click.echo(f"\n✅ RECOMMENDATION: Proceed with bet")
    else:
        click.echo(f"\n❌ RECOMMENDATION: Avoid betting at this timing")


def _display_summary_table(summary_data):
    """Display timing performance summary table."""
    
    table_data = []
    for row in summary_data:
        table_data.append([
            row['timing_bucket'],
            row['source'] or 'All',
            row['split_type'] or 'All',
            row['strategy_name'] or 'All',
            row['total_bets'],
            f"{row['win_rate']:.1f}%",
            f"{row['roi_percentage']:.1f}%",
            row['confidence_level'],
            row['performance_grade'],
            row['recommendation_confidence']
        ])
    
    headers = [
        "Timing", "Source", "Type", "Strategy", 
        "Bets", "Win Rate", "ROI", "Confidence", 
        "Grade", "Rec Confidence"
    ]
    
    click.echo(tabulate(table_data, headers=headers, tablefmt="grid"))
    
    # Summary stats
    total_bets = sum(row['total_bets'] for row in summary_data)
    avg_win_rate = sum(row['win_rate'] * row['total_bets'] for row in summary_data) / total_bets if total_bets > 0 else 0
    avg_roi = sum(row['roi_percentage'] * row['total_bets'] for row in summary_data) / total_bets if total_bets > 0 else 0
    
    click.echo(f"\n📈 SUMMARY TOTALS:")
    click.echo(f"   Total bets: {total_bets}")
    click.echo(f"   Weighted avg win rate: {avg_win_rate:.1f}%")
    click.echo(f"   Weighted avg ROI: {avg_roi:.1f}%")
    
    # Best timing bucket
    if summary_data:
        best_bucket = max(summary_data, key=lambda x: x['roi_percentage'])
        click.echo(f"   Best timing: {best_bucket['timing_bucket']} ({best_bucket['roi_percentage']:.1f}% ROI)")


def _export_json_analysis(analysis, output_path: Optional[Path]):
    """Export analysis to JSON format."""
    import json
    
    # Convert Pydantic model to dict
    json_data = analysis.model_dump()
    
    # Convert Decimal objects to float
    def decimal_converter(obj):
        if hasattr(obj, 'to_eng_string'):  # Decimal check
            return float(obj)
        return str(obj)
    
    json_str = json.dumps(json_data, indent=2, default=decimal_converter)
    
    if output_path:
        output_path.write_text(json_str)
        click.echo(f"✅ JSON analysis exported to: {output_path}")
    else:
        click.echo(json_str)


def _export_csv_analysis(analysis, output_path: Optional[Path]):
    """Export analysis to CSV format."""
    import csv
    
    if not output_path:
        click.echo("❌ CSV format requires --output option", err=True)
        sys.exit(1)
    
    with output_path.open('w', newline='') as csvfile:
        writer = csv.writer(csvfile)
        
        # Write header
        writer.writerow([
            'Timing Bucket', 'Source', 'Book', 'Split Type', 'Strategy',
            'Total Bets', 'Wins', 'Losses', 'Pushes', 'Win Rate %',
            'ROI %', 'Profit/Loss Units', 'Confidence Level', 'Performance Grade'
        ])
        
        # Write bucket data
        for bucket_analysis in analysis.bucket_analyses:
            metrics = bucket_analysis.metrics
            writer.writerow([
                bucket_analysis.timing_bucket.display_name if hasattr(bucket_analysis.timing_bucket, 'display_name') else bucket_analysis.timing_bucket,
                bucket_analysis.source.value if hasattr(bucket_analysis.source, 'value') and bucket_analysis.source else (bucket_analysis.source if bucket_analysis.source else 'All'),
                bucket_analysis.book.value if hasattr(bucket_analysis.book, 'value') and bucket_analysis.book else (bucket_analysis.book if bucket_analysis.book else 'All'),
                bucket_analysis.split_type.value if hasattr(bucket_analysis.split_type, 'value') and bucket_analysis.split_type else (bucket_analysis.split_type if bucket_analysis.split_type else 'All'),
                bucket_analysis.strategy_name or 'All',
                metrics.total_bets,
                metrics.wins,
                metrics.losses,
                metrics.pushes,
                f"{metrics.win_rate:.1f}",
                f"{metrics.roi_percentage:.1f}",
                f"{metrics.total_profit_loss:.2f}",
                metrics.confidence_level.value,
                bucket_analysis.performance_grade
            ])
    
    click.echo(f"✅ CSV analysis exported to: {output_path}")


# Export the group for CLI integration
__all__ = ['timing_group'] 