"""
CLI commands for ML Pipeline management and validation.
Addresses Issue #55: ML Pipeline Integration Crisis.
"""

import asyncio
import click
from typing import Optional

from src.core.config import get_settings
from src.data.database.connection import get_database_connection


@click.group()
def ml_pipeline():
    """ML Pipeline management and validation commands."""
    pass


@ml_pipeline.command()
@click.option('--validate-only', is_flag=True, help='Only validate pipeline, do not populate features')
@click.option('--force', is_flag=True, help='Force repopulation even if features exist')
def populate_features(validate_only: bool, force: bool):
    """Populate ML features from production data."""
    
    async def _populate_features():
        config = get_settings()
        
        async with get_database_connection(config) as conn:
            if validate_only:
                click.echo("🔍 Validating ML pipeline data flow...")
                result = await conn.fetch("SELECT * FROM validate_ml_pipeline_data_flow();")
                
                click.echo("\n📊 ML Pipeline Validation Results:")
                for row in result:
                    status_color = "green" if row['status'] == 'PASS' else "red"
                    click.echo(f"  {row['metric_name']}: {row['current_value']} / {row['expected_minimum']} - ", nl=False)
                    click.secho(row['status'], fg=status_color)
                    click.echo(f"    {row['details']}")
                
                # Summary
                pass_count = sum(1 for row in result if row['status'] == 'PASS')
                total_count = len(result)
                
                if pass_count == total_count:
                    click.secho(f"\n✅ ML Pipeline Health: {pass_count}/{total_count} checks passed", fg="green")
                else:
                    click.secho(f"\n⚠️  ML Pipeline Health: {pass_count}/{total_count} checks passed", fg="yellow")
                
                return
            
            if force:
                click.echo("🗑️  Clearing existing ML features...")
                await conn.execute("DELETE FROM curated.ml_features;")
            
            click.echo("🔧 Populating ML features from production data...")
            result = await conn.fetchrow("SELECT * FROM populate_ml_features_from_production_data();")
            
            click.echo(f"\n📈 Population Results:")
            click.echo(f"  Processed Games: {result['processed_games']}")
            click.echo(f"  Features Created: {result['inserted_features']}")
            click.echo(f"  Skipped Existing: {result['skipped_existing']}")
            
            if result['processing_errors'] > 0:
                click.secho(f"  Processing Errors: {result['processing_errors']}", fg="red")
            else:
                click.secho(f"  Processing Errors: {result['processing_errors']}", fg="green")
            
            # Validate after population
            validation_result = await conn.fetch("SELECT * FROM validate_ml_pipeline_data_flow();")
            
            click.echo("\n🔍 Post-Population Validation:")
            for row in validation_result:
                status_color = "green" if row['status'] == 'PASS' else "red"
                click.echo(f"  {row['metric_name']}: ", nl=False)
                click.secho(f"{row['current_value']}", fg=status_color)
    
    asyncio.run(_populate_features())


@ml_pipeline.command()
def health():
    """Check ML pipeline health and data flow."""
    
    async def _health_check():
        config = get_settings()
        
        async with get_database_connection(config) as conn:
            click.echo("🏥 ML Pipeline Health Check")
            click.echo("=" * 40)
            
            # Get health metrics
            health_data = await conn.fetch("SELECT * FROM analytics.ml_pipeline_health;")
            
            for component in health_data:
                click.echo(f"\n📊 {component['component']}:")
                click.echo(f"  Records: {component['record_count']:,}")
                if component['avg_quality_score']:
                    click.echo(f"  Quality Score: {component['avg_quality_score']:.3f}")
                click.echo(f"  High Quality: {component['high_quality_count']:,}")
                if component['earliest_record']:
                    click.echo(f"  Date Range: {component['earliest_record']} to {component['latest_record']}")
            
            # Check for recent activity
            click.echo(f"\n🕒 Recent Activity Check:")
            recent_features = await conn.fetchval(
                "SELECT COUNT(*) FROM curated.ml_features WHERE feature_extraction_date > NOW() - INTERVAL '24 hours'"
            )
            recent_games = await conn.fetchval(
                "SELECT COUNT(*) FROM curated.enhanced_games WHERE created_at > NOW() - INTERVAL '24 hours'"
            )
            
            if recent_features > 0:
                click.secho(f"  Recent ML Features: {recent_features}", fg="green")
            else:
                click.secho(f"  Recent ML Features: {recent_features}", fg="yellow")
            
            if recent_games > 0:
                click.secho(f"  Recent Games: {recent_games}", fg="green")
            else:
                click.secho(f"  Recent Games: {recent_games}", fg="yellow")
    
    asyncio.run(_health_check())


@ml_pipeline.command()
@click.option('--sample-size', default=5, help='Number of feature samples to show')
def show_features(sample_size: int):
    """Show sample ML features data."""
    
    async def _show_features():
        config = get_settings()
        
        async with get_database_connection(config) as conn:
            features = await conn.fetch(f"""
                SELECT 
                    game_id, home_team, away_team, game_date,
                    opening_moneyline_home, opening_moneyline_away,
                    opening_spread, opening_total,
                    sharp_money_percentage_home, reverse_line_movement,
                    data_quality_score, missing_features_count
                FROM curated.ml_features 
                ORDER BY feature_extraction_date DESC 
                LIMIT {sample_size}
            """)
            
            if not features:
                click.secho("❌ No ML features found!", fg="red")
                return
            
            click.echo(f"🎯 Latest {sample_size} ML Features:")
            click.echo("=" * 80)
            
            for feature in features:
                click.echo(f"\nGame ID: {feature['game_id']}")
                click.echo(f"Teams: {feature['away_team']} @ {feature['home_team']}")
                click.echo(f"Date: {feature['game_date']}")
                click.echo(f"Moneyline: {feature['opening_moneyline_away'] or 'N/A'} / {feature['opening_moneyline_home'] or 'N/A'}")
                click.echo(f"Spread: {feature['opening_spread'] or 'N/A'}")
                click.echo(f"Total: {feature['opening_total'] or 'N/A'}")
                click.echo(f"Sharp %: {feature['sharp_money_percentage_home'] or 'N/A'}")
                click.echo(f"RLM: {feature['reverse_line_movement']}")
                
                quality_color = "green" if feature['data_quality_score'] >= 0.8 else "yellow" if feature['data_quality_score'] >= 0.6 else "red"
                click.echo(f"Quality: ", nl=False)
                click.secho(f"{feature['data_quality_score']:.3f}", fg=quality_color)
                click.echo(f"Missing: {feature['missing_features_count']}")
    
    asyncio.run(_show_features())


@ml_pipeline.command()
@click.option('--test-training', is_flag=True, help='Test ML training pipeline with new features')
def validate_training(test_training: bool):
    """Validate that ML training pipeline can use the populated features."""
    
    async def _validate_training():
        config = get_settings()
        
        try:
            click.echo("🧪 Testing ML Training Pipeline...")
            
            # Test database connectivity first
            async with get_database_connection(config) as conn:
                click.echo("📊 Loading features from database...")
                
                features = await conn.fetch("""
                    SELECT COUNT(*) as count, 
                           MIN(game_date) as min_date, 
                           MAX(game_date) as max_date,
                           AVG(data_quality_score) as avg_quality
                    FROM curated.ml_features
                """)
                
                if features and features[0]['count'] > 0:
                    feature_info = features[0]
                    click.secho(f"✅ Found {feature_info['count']} ML feature records", fg="green")
                    click.echo(f"   Date range: {feature_info['min_date']} to {feature_info['max_date']}")
                    click.echo(f"   Average quality: {feature_info['avg_quality']:.3f}")
                    
                    if test_training:
                        click.echo("🤖 Testing ML training readiness...")
                        
                        # Check for required columns
                        sample_features = await conn.fetch("""
                            SELECT game_id, home_team, away_team, game_date, 
                                   opening_moneyline_home, opening_moneyline_away,
                                   data_quality_score
                            FROM curated.ml_features 
                            LIMIT 1
                        """)
                        
                        if sample_features:
                            sample = sample_features[0]
                            required_fields = ['game_id', 'home_team', 'away_team', 'game_date']
                            
                            missing_fields = [field for field in required_fields if sample[field] is None]
                            
                            if missing_fields:
                                click.secho(f"❌ Missing required fields: {missing_fields}", fg="red")
                            else:
                                click.secho("✅ All required fields present for ML training", fg="green")
                                
                                # Check data completeness
                                complete_records = await conn.fetchval("""
                                    SELECT COUNT(*) FROM curated.ml_features 
                                    WHERE opening_moneyline_home IS NOT NULL 
                                       OR opening_moneyline_away IS NOT NULL
                                       OR opening_spread IS NOT NULL
                                """)
                                
                                click.echo(f"📈 Records with betting data: {complete_records}")
                                
                                if complete_records > 10:
                                    click.secho("✅ Sufficient training data available", fg="green")
                                else:
                                    click.secho("⚠️  Limited training data - may need more betting lines", fg="yellow")
                else:
                    click.secho("❌ No ML features found in database", fg="red")
                    click.echo("   Run: uv run -m src.interfaces.cli ml-pipeline populate-features")
                    
        except Exception as e:
            click.secho(f"❌ Error validating ML pipeline: {str(e)}", fg="red")
    
    asyncio.run(_validate_training())


if __name__ == '__main__':
    ml_pipeline()