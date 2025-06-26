#!/usr/bin/env python3
"""
Setup script for MLB Betting Recommendation Timing Analysis System.

This script sets up the database schema and provides examples of how to use
the timing analysis system for evaluating betting recommendation performance.
"""

import asyncio
import sys
from datetime import datetime, timedelta, timezone
from pathlib import Path

import click
import structlog

# Add src to path for imports
sys.path.insert(0, 'src')

from mlb_sharp_betting.db.connection import get_db_manager
from mlb_sharp_betting.services.timing_analysis_service import TimingAnalysisService
from mlb_sharp_betting.models.timing_analysis import RealtimeTimingLookup
from mlb_sharp_betting.models.splits import SplitType, DataSource, BookType
from mlb_sharp_betting.analyzers.timing_recommendation_tracker import TimingRecommendationTracker

logger = structlog.get_logger(__name__)


async def setup_database_schema():
    """Setup the timing analysis database schema."""
    click.echo("🔧 Setting up timing analysis database schema...")
    
    db_manager = get_db_manager()
    
    try:
        # Read and execute the schema file
        schema_file = Path("sql/timing_analysis_schema.sql")
        if not schema_file.exists():
            click.echo(f"❌ Schema file not found: {schema_file}")
            return False
        
        schema_sql = schema_file.read_text()
        
        with db_manager.get_cursor() as cursor:
            # Execute the schema creation
            cursor.execute(schema_sql)
        
        click.echo("✅ Database schema created successfully")
        return True
        
    except Exception as e:
        click.echo(f"❌ Failed to create database schema: {e}")
        return False
    finally:
        db_manager.close()


async def create_sample_data():
    """Create sample timing analysis data for demonstration."""
    click.echo("📊 Creating sample timing analysis data...")
    
    db_manager = get_db_manager()
    timing_tracker = TimingRecommendationTracker(db_manager)
    
    try:
        # Sample game data
        sample_games = [
            {
                'game_id': 'sample_game_001',
                'home_team': 'LAD',
                'away_team': 'SF',
                'game_datetime': datetime.now(timezone.utc) + timedelta(hours=6),
            },
            {
                'game_id': 'sample_game_002', 
                'home_team': 'NYY',
                'away_team': 'BOS',
                'game_datetime': datetime.now(timezone.utc) + timedelta(hours=12),
            },
            {
                'game_id': 'sample_game_003',
                'home_team': 'HOU',
                'away_team': 'LAA',
                'game_datetime': datetime.now(timezone.utc) + timedelta(hours=24),
            }
        ]
        
        # Sample strategies and their characteristics
        strategies = [
            {'name': 'sharp_action_strategy', 'success_rate': 0.58, 'typical_odds': -110},
            {'name': 'opposing_markets_strategy', 'success_rate': 0.62, 'typical_odds': -105},
            {'name': 'steam_move_strategy', 'success_rate': 0.55, 'typical_odds': -115},
            {'name': 'consensus_fade_strategy', 'success_rate': 0.53, 'typical_odds': -110}
        ]
        
        # Create sample recommendations across different timing buckets
        recommendations_created = 0
        
        for game in sample_games:
            for strategy in strategies:
                # Create recommendations at different times before the game
                timing_offsets = [1, 3, 8, 36]  # Hours before game
                
                for hours_before in timing_offsets:
                    rec_time = game['game_datetime'] - timedelta(hours=hours_before)
                    
                    # Track a moneyline recommendation
                    success = await timing_tracker.track_recommendation_from_signal(
                        game_id=game['game_id'],
                        home_team=game['home_team'],
                        away_team=game['away_team'],
                        game_datetime=game['game_datetime'],
                        signal_type=strategy['name'],
                        signal_source='VSIN',
                        signal_book='circa',
                        bet_type='moneyline',
                        recommended_side='home',
                        signal_strength=1.0,
                        odds=strategy['typical_odds']
                    )
                    
                    if success:
                        recommendations_created += 1
                    
                    # Also create a spread recommendation
                    success = await timing_tracker.track_recommendation_from_signal(
                        game_id=game['game_id'],
                        home_team=game['home_team'],
                        away_team=game['away_team'],
                        game_datetime=game['game_datetime'],
                        signal_type=strategy['name'],
                        signal_source='VSIN',
                        signal_book='draftkings',
                        bet_type='spread',
                        recommended_side='away',
                        signal_strength=1.0,
                        odds=-110
                    )
                    
                    if success:
                        recommendations_created += 1
        
        click.echo(f"✅ Created {recommendations_created} sample recommendations")
        return True
        
    except Exception as e:
        click.echo(f"❌ Failed to create sample data: {e}")
        return False
    finally:
        db_manager.close()


async def demonstrate_timing_analysis():
    """Demonstrate the timing analysis functionality."""
    click.echo("🎯 Demonstrating timing analysis functionality...")
    
    db_manager = get_db_manager()
    timing_service = TimingAnalysisService(db_manager)
    
    try:
        # 1. Get timing performance summary
        click.echo("\n📊 Current Timing Performance Summary:")
        summary_data = await timing_service.get_timing_performance_summary(
            days_back=7,
            minimum_sample_size=1  # Lower threshold for demo
        )
        
        if summary_data:
            for row in summary_data[:5]:  # Show top 5
                click.echo(f"   {row['timing_bucket']} | {row['split_type']} | {row['total_bets']} bets | {row['win_rate']:.1f}% WR | {row['roi_percentage']:.1f}% ROI")
        else:
            click.echo("   No timing performance data available yet")
        
        # 2. Get real-time timing recommendation
        click.echo("\n🎯 Real-time Timing Recommendation Example:")
        
        lookup = RealtimeTimingLookup(
            hours_until_game=3.5,
            source=DataSource.VSIN,
            book=BookType.CIRCA,
            split_type=SplitType.MONEYLINE,
            strategy_name='sharp_action_strategy'
        )
        
        recommendation = await timing_service.get_realtime_timing_recommendation(lookup)
        
        click.echo(f"   Timing bucket: {lookup.timing_bucket.display_name}")
        click.echo(f"   Confidence: {recommendation.confidence}")
        click.echo(f"   Action: {recommendation.action_needed}")
        click.echo(f"   Recommendation: {recommendation.recommendation[:100]}...")
        
        # 3. Run comprehensive timing analysis
        click.echo("\n📈 Running Comprehensive Timing Analysis:")
        
        end_date = datetime.now(timezone.utc)
        start_date = end_date - timedelta(days=7)
        
        analysis = await timing_service.analyze_timing_performance(
            start_date=start_date,
            end_date=end_date,
            minimum_sample_size=1
        )
        
        click.echo(f"   Analysis: {analysis.analysis_name}")
        click.echo(f"   Games analyzed: {analysis.total_games_analyzed}")
        click.echo(f"   Total recommendations: {analysis.total_recommendations}")
        click.echo(f"   Timing buckets analyzed: {len(analysis.bucket_analyses)}")
        
        if analysis.best_bucket:
            click.echo(f"   Best timing: {analysis.best_bucket.display_name}")
        
        click.echo(f"   Optimal timing recommendation: {analysis.optimal_timing_recommendation}")
        
        return True
        
    except Exception as e:
        click.echo(f"❌ Failed to demonstrate timing analysis: {e}")
        return False
    finally:
        db_manager.close()


async def show_cli_examples():
    """Show examples of CLI commands for timing analysis."""
    click.echo("\n🖥️  CLI COMMAND EXAMPLES:")
    click.echo("=" * 80)
    
    examples = [
        {
            'title': '📊 Analyze Timing Performance',
            'command': 'uv run python -m mlb_sharp_betting.cli timing analyze --days-back 30 --min-sample-size 10',
            'description': 'Analyze betting recommendation timing performance for the last 30 days'
        },
        {
            'title': '🎯 Get Real-time Timing Recommendation',
            'command': 'uv run python -m mlb_sharp_betting.cli timing recommend --hours-until-game 4.5 --split-type moneyline --source VSIN',
            'description': 'Get timing recommendation for a moneyline bet 4.5 hours before game start'
        },
        {
            'title': '📋 Show Timing Performance Summary',
            'command': 'uv run python -m mlb_sharp_betting.cli timing summary --days-back 30',
            'description': 'Show summary of timing performance across all buckets'
        },
        {
            'title': '🔄 Update Recommendation Outcomes',
            'command': 'uv run python -m mlb_sharp_betting.cli timing update-outcomes',
            'description': 'Update recommendation outcomes using game results'
        },
        {
            'title': '📝 Track a Recommendation',
            'command': 'uv run python -m mlb_sharp_betting.cli timing track --game-id "game123" --home-team "LAD" --away-team "SF" --game-datetime "2024-01-15 19:00" --source VSIN --split-type moneyline --strategy "sharp_action" --recommended-side home',
            'description': 'Manually track a betting recommendation for timing analysis'
        },
        {
            'title': '📊 Export Analysis to JSON',
            'command': 'uv run python -m mlb_sharp_betting.cli timing analyze --format json --output timing_analysis.json',
            'description': 'Export timing analysis results to JSON file'
        },
        {
            'title': '🔍 Filter Analysis by Strategy',
            'command': 'uv run python -m mlb_sharp_betting.cli timing analyze --strategy "sharp_action_strategy" --source VSIN',
            'description': 'Analyze timing performance for specific strategy and source'
        }
    ]
    
    for example in examples:
        click.echo(f"\n{example['title']}:")
        click.echo(f"  Command: {example['command']}")
        click.echo(f"  Purpose: {example['description']}")
    
    click.echo("\n💡 TIP: Use --help with any command to see all available options")
    click.echo("💡 TIP: Set up the timing analysis schema first with this setup script")


async def main():
    """Main setup function."""
    click.echo("🏀 MLB BETTING RECOMMENDATION TIMING ANALYSIS SYSTEM SETUP")
    click.echo("=" * 80)
    click.echo("This system analyzes the performance of betting recommendations")
    click.echo("based on their timing relative to game start, enabling data-driven")
    click.echo("decision making for optimal bet placement timing.\n")
    
    # Step 1: Setup database schema
    success = await setup_database_schema()
    if not success:
        click.echo("❌ Setup failed at database schema creation")
        return
    
    # Step 2: Create sample data
    click.echo("\n" + "="*50)
    create_sample = click.confirm("Create sample data for demonstration?", default=True)
    if create_sample:
        success = await create_sample_data()
        if not success:
            click.echo("⚠️  Sample data creation failed, but schema is ready")
    
    # Step 3: Demonstrate functionality  
    if create_sample:
        click.echo("\n" + "="*50)
        demonstrate = click.confirm("Run demonstration of timing analysis?", default=True)
        if demonstrate:
            await demonstrate_timing_analysis()
    
    # Step 4: Show CLI examples
    click.echo("\n" + "="*50)
    await show_cli_examples()
    
    click.echo("\n✅ SETUP COMPLETE!")
    click.echo("🎯 The timing analysis system is now ready to use.")
    click.echo("📊 Use the CLI commands shown above to analyze timing performance.")
    click.echo("💡 Integrate with the master betting detector to automatically track recommendations.")


@click.command()
@click.option('--schema-only', is_flag=True, help='Only create database schema')
@click.option('--demo-only', is_flag=True, help='Only run demonstration (requires existing schema)')
def setup(schema_only: bool, demo_only: bool):
    """Setup the MLB betting recommendation timing analysis system."""
    
    async def run_setup():
        if demo_only:
            await demonstrate_timing_analysis()
            await show_cli_examples()
        elif schema_only:
            await setup_database_schema()
        else:
            await main()
    
    asyncio.run(run_setup())


if __name__ == "__main__":
    setup() 