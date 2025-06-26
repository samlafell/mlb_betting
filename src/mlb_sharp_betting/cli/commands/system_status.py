"""
Comprehensive system status and health checking commands.
"""

import click
import asyncio
from datetime import datetime
from typing import Dict, Any
import structlog

from ...services.pipeline_orchestrator import PipelineOrchestrator
from ...services.enhanced_backtesting_service import EnhancedBacktestingService
from ...db.connection import get_db_manager
from ...services.data_persistence import DataPersistenceService

logger = structlog.get_logger(__name__)


@click.group() 
def status_group():
    """🔧 System status and health checking commands."""
    pass


@status_group.command('overview')
def system_status():
    """📊 Show complete system status including data freshness"""
    
    async def show_system_status():
        click.echo("📊 SYSTEM STATUS OVERVIEW")
        click.echo("=" * 60)
        
        try:
            # Initialize services
            db_manager = get_db_manager()
            orchestrator = PipelineOrchestrator(db_manager)
            enhanced_service = EnhancedBacktestingService(db_manager, auto_collect_data=False)
            
            # Get system state analysis
            click.echo("🔍 Analyzing system state...")
            system_state = await orchestrator.analyze_system_state()
            
            # Display system health
            health = system_state['system_health']
            health_emoji = {
                'excellent': '🟢',
                'good': '🟡', 
                'fair': '🟠',
                'poor': '🔴',
                'unknown': '⚪'
            }.get(health, '⚪')
            
            click.echo(f"\n{health_emoji} OVERALL SYSTEM HEALTH: {health.upper()}")
            
            # Data status
            click.echo(f"\n📡 DATA STATUS:")
            if system_state['data_age_hours'] is not None:
                age_status = "✅" if not system_state['needs_data_collection'] else "⚠️"
                click.echo(f"   {age_status} Data Age: {system_state['data_age_hours']:.1f} hours")
                click.echo(f"   📊 Collection Needed: {'Yes' if system_state['needs_data_collection'] else 'No'}")
            else:
                click.echo(f"   ❌ No data available")
            
            # Backtesting status
            click.echo(f"\n🔬 BACKTESTING STATUS:")
            if system_state['backtesting_age_hours'] is not None:
                bt_status = "✅" if not system_state['needs_backtesting'] else "⚠️"
                click.echo(f"   {bt_status} Backtesting Age: {system_state['backtesting_age_hours']:.1f} hours")
                click.echo(f"   🔄 Backtesting Needed: {'Yes' if system_state['needs_backtesting'] else 'No'}")
            else:
                click.echo(f"   ❌ No backtesting results available")
            
            # Data quality issues
            if system_state['data_quality_issues']:
                click.echo(f"\n⚠️  DATA QUALITY ISSUES:")
                for issue in system_state['data_quality_issues']:
                    click.echo(f"   • {issue}")
            
            # Recommendations
            if system_state['recommendations']:
                click.echo(f"\n💡 RECOMMENDATIONS:")
                for rec in system_state['recommendations']:
                    click.echo(f"   • {rec}")
            
            # Get detailed data freshness
            click.echo(f"\n📊 DETAILED DATA METRICS:")
            freshness_check = await enhanced_service.check_data_freshness()
            
            click.echo(f"   📈 Total Splits: {freshness_check.get('total_splits', 0):,}")
            click.echo(f"   🎮 Unique Games: {freshness_check.get('unique_games', 0)}")
            click.echo(f"   🏆 Game Outcomes: {freshness_check.get('total_outcomes', 0)}")
            
            # Database connection
            try:
                with db_manager.get_cursor() as cursor:
                    cursor.execute("SELECT 1")
                    click.echo(f"\n✅ DATABASE CONNECTION: OK")
                    click.echo(f"   📁 Database: PostgreSQL (mlb_betting)")
            except Exception as e:
                click.echo(f"\n❌ DATABASE CONNECTION: FAILED ({e})")
            
            # Pipeline recommendations
            recommendations = await orchestrator.get_pipeline_recommendations()
            click.echo(f"\n🎚️  PIPELINE PRIORITY: {recommendations['priority_level'].upper()}")
            click.echo(f"⏱️  ESTIMATED RUNTIME: {recommendations['estimated_runtime_minutes']} minutes")
            
            if recommendations['immediate_actions']:
                click.echo(f"\n🚨 IMMEDIATE ACTIONS NEEDED:")
                for action in recommendations['immediate_actions']:
                    click.echo(f"   • {action['action'].title()}: {action['reason']}")
            else:
                click.echo(f"\n✅ NO IMMEDIATE ACTIONS NEEDED")
            
        except Exception as e:
            click.echo(f"❌ System status check failed: {e}")
        finally:
            try:
                if 'orchestrator' in locals():
                    orchestrator.close()
                if 'db_manager' in locals():
                    db_manager.close()
            except Exception:
                pass
    
    try:
        asyncio.run(show_system_status())
    except Exception:
        click.echo("❌ System status check failed")
        raise


@status_group.command('health')
@click.option('--detailed', is_flag=True, help='Show detailed health information')
def health_check(detailed: bool):
    """🏥 Run comprehensive system health check"""
    
    async def run_health_check():
        click.echo("🏥 COMPREHENSIVE HEALTH CHECK")
        click.echo("=" * 60)
        
        health_results = {
            'database': False,
            'schema': False,
            'data_pipeline': False,
            'backtesting': False,
            'data_freshness': False,
            'data_quality': False
        }
        
        issues = []
        warnings = []
        
        try:
            # Test 1: Database Connection
            click.echo("🔍 Testing database connection...")
            try:
                db_manager = get_db_manager()
                with db_manager.get_cursor() as cursor:
                    cursor.execute("SELECT 1")
                    cursor.execute("SELECT version()")
                    version = cursor.fetchone()[0]
                    
                health_results['database'] = True
                click.echo("   ✅ Database connection successful")
                if detailed:
                    click.echo(f"      📋 Version: {version}")
                    
            except Exception as e:
                click.echo(f"   ❌ Database connection failed: {e}")
                issues.append(f"Database connection: {e}")
            
            # Test 2: Schema Validation
            click.echo("🔍 Validating database schema...")
            try:
                with db_manager.get_cursor() as cursor:
                    # Check required schemas
                    cursor.execute("""
                        SELECT schema_name 
                        FROM information_schema.schemata 
                        WHERE schema_name IN ('splits', 'main', 'backtesting')
                    """)
                    schemas = [row['schema_name'] for row in cursor.fetchall()]
                    
                    required_schemas = ['splits', 'main']
                    missing_schemas = [s for s in required_schemas if s not in schemas]
                    
                    if not missing_schemas:
                        health_results['schema'] = True
                        click.echo("   ✅ Database schema validation passed")
                        if detailed:
                            click.echo(f"      📋 Found schemas: {', '.join(schemas)}")
                    else:
                        click.echo(f"   ❌ Missing schemas: {', '.join(missing_schemas)}")
                        issues.append(f"Missing schemas: {', '.join(missing_schemas)}")
                        
            except Exception as e:
                click.echo(f"   ❌ Schema validation failed: {e}")
                issues.append(f"Schema validation: {e}")
            
            # Test 3: Data Pipeline
            click.echo("🔍 Testing data pipeline...")
            try:
                from ...entrypoint import DataPipeline
                
                # Test pipeline initialization with dry run
                test_pipeline = DataPipeline(sport='mlb', sportsbook='circa', dry_run=True)
                
                health_results['data_pipeline'] = True
                click.echo("   ✅ Data pipeline initialization successful")
                
            except Exception as e:
                click.echo(f"   ❌ Data pipeline test failed: {e}")
                issues.append(f"Data pipeline: {e}")
            
            # Test 4: Backtesting Service
            click.echo("🔍 Testing backtesting service...")
            try:
                enhanced_service = EnhancedBacktestingService(auto_collect_data=False)
                validations = await enhanced_service.validate_pipeline_requirements()
                
                if all(validations.values()):
                    health_results['backtesting'] = True
                    click.echo("   ✅ Backtesting service validation passed")
                else:
                    failed_reqs = [k for k, v in validations.items() if not v]
                    click.echo(f"   ⚠️  Backtesting validation issues: {', '.join(failed_reqs)}")
                    warnings.append(f"Backtesting issues: {', '.join(failed_reqs)}")
                    
            except Exception as e:
                click.echo(f"   ❌ Backtesting service test failed: {e}")
                issues.append(f"Backtesting service: {e}")
            
            # Test 5: Data Freshness
            click.echo("🔍 Checking data freshness...")
            try:
                enhanced_service = EnhancedBacktestingService()
                freshness_check = await enhanced_service.check_data_freshness()
                
                if freshness_check['is_fresh']:
                    health_results['data_freshness'] = True
                    click.echo(f"   ✅ Data is fresh ({freshness_check['data_age_hours']:.1f} hours old)")
                else:
                    click.echo(f"   ⚠️  Data is stale ({freshness_check.get('data_age_hours', 'unknown')} hours old)")
                    warnings.append(f"Data is stale - collection recommended")
                    
                if detailed:
                    click.echo(f"      📊 Total splits: {freshness_check.get('total_splits', 0):,}")
                    click.echo(f"      🎮 Unique games: {freshness_check.get('unique_games', 0)}")
                    
            except Exception as e:
                click.echo(f"   ❌ Data freshness check failed: {e}")
                issues.append(f"Data freshness: {e}")
            
            # Test 6: Data Quality
            click.echo("🔍 Analyzing data quality...")
            try:
                persistence_service = DataPersistenceService(db_manager)
                integrity_results = persistence_service.verify_data_integrity()
                
                if integrity_results['overall_health'] == 'good':
                    health_results['data_quality'] = True
                    click.echo("   ✅ Data quality check passed")
                else:
                    click.echo(f"   ⚠️  Data quality issues detected")
                    warnings.extend(integrity_results.get('warnings', []))
                    issues.extend(integrity_results.get('errors', []))
                    
                if detailed:
                    click.echo(f"      ✅ Checks passed: {integrity_results['checks_passed']}")
                    click.echo(f"      ❌ Checks failed: {integrity_results['checks_failed']}")
                    
            except Exception as e:
                click.echo(f"   ❌ Data quality check failed: {e}")
                issues.append(f"Data quality: {e}")
            
            # Summary
            passed_checks = sum(health_results.values())
            total_checks = len(health_results)
            
            click.echo(f"\n📊 HEALTH CHECK SUMMARY:")
            click.echo(f"   ✅ Passed: {passed_checks}/{total_checks} checks")
            click.echo(f"   ❌ Failed: {total_checks - passed_checks}/{total_checks} checks")
            click.echo(f"   ⚠️  Warnings: {len(warnings)}")
            
            # Overall health rating
            health_percentage = (passed_checks / total_checks) * 100
            
            if health_percentage >= 90:
                overall_health = "🟢 EXCELLENT"
            elif health_percentage >= 75:
                overall_health = "🟡 GOOD"
            elif health_percentage >= 50:
                overall_health = "🟠 FAIR"
            else:
                overall_health = "🔴 POOR"
            
            click.echo(f"\n{overall_health} OVERALL HEALTH: {health_percentage:.0f}%")
            
            # Show issues and warnings
            if issues:
                click.echo(f"\n❌ CRITICAL ISSUES:")
                for issue in issues:
                    click.echo(f"   • {issue}")
                    
                click.echo(f"\n💡 Run 'mlb-cli status fix' to attempt automatic fixes")
            
            if warnings:
                click.echo(f"\n⚠️  WARNINGS:")
                for warning in warnings:
                    click.echo(f"   • {warning}")
            
            if not issues and not warnings:
                click.echo(f"\n🎉 System is healthy and ready for operation!")
                
        except Exception as e:
            click.echo(f"❌ Health check failed: {e}")
        finally:
            try:
                if 'db_manager' in locals():
                    db_manager.close()
            except Exception:
                pass
    
    try:
        asyncio.run(run_health_check())
    except Exception:
        click.echo("❌ Health check failed")
        raise


@status_group.command('performance')
def performance_metrics():
    """📈 Show system performance metrics"""
    
    async def show_performance():
        click.echo("📈 SYSTEM PERFORMANCE METRICS")
        click.echo("=" * 60)
        
        try:
            db_manager = get_db_manager()
            
            # Query performance metrics
            with db_manager.get_cursor() as cursor:
                # Database size metrics
                cursor.execute("""
                    SELECT 
                        schemaname,
                        tablename,
                        pg_size_pretty(pg_total_relation_size(schemaname||'.'||tablename)) as size,
                        pg_total_relation_size(schemaname||'.'||tablename) as size_bytes
                    FROM pg_tables 
                    WHERE schemaname IN ('splits', 'main', 'backtesting')
                    ORDER BY size_bytes DESC
                """)
                table_sizes = cursor.fetchall()
                
                if table_sizes:
                    click.echo("💾 DATABASE SIZE METRICS:")
                    total_size = sum(row['size_bytes'] for row in table_sizes)
                    click.echo(f"   📊 Total Size: {total_size / (1024*1024):.1f} MB")
                    
                    for row in table_sizes[:5]:  # Top 5 largest tables
                        click.echo(f"   📋 {row['schemaname']}.{row['tablename']}: {row['size']}")
                
                # Record counts
                cursor.execute("SELECT COUNT(*) as splits_count FROM splits.raw_mlb_betting_splits")
                splits_count = cursor.fetchone()['splits_count']
                
                cursor.execute("SELECT COUNT(*) as games_count FROM main.games")
                games_count = cursor.fetchone()['games_count'] if cursor.rowcount > 0 else 0
                
                cursor.execute("SELECT COUNT(*) as outcomes_count FROM public.game_outcomes")
                outcomes_count = cursor.fetchone()['outcomes_count'] if cursor.rowcount > 0 else 0
                
                click.echo(f"\n📊 RECORD COUNTS:")
                click.echo(f"   📈 Betting Splits: {splits_count:,}")
                click.echo(f"   🎮 Games: {games_count:,}")
                click.echo(f"   🏆 Game Outcomes: {outcomes_count:,}")
                
                # Recent activity (last 24 hours)
                cursor.execute("""
                    SELECT COUNT(*) as recent_splits
                    FROM splits.raw_mlb_betting_splits
                    WHERE last_updated >= NOW() - INTERVAL '24 hours'
                """)
                recent_splits = cursor.fetchone()['recent_splits']
                
                click.echo(f"\n⏰ RECENT ACTIVITY (24h):")
                click.echo(f"   📈 New Splits: {recent_splits:,}")
                
                # Performance indicators
                if splits_count > 0:
                    data_density = games_count / splits_count if splits_count > 0 else 0
                    outcome_coverage = (outcomes_count / games_count * 100) if games_count > 0 else 0
                    
                    click.echo(f"\n📊 PERFORMANCE INDICATORS:")
                    click.echo(f"   🎯 Data Density: {data_density:.2f} games per split")
                    click.echo(f"   🏆 Outcome Coverage: {outcome_coverage:.1f}%")
                    
                    # Data quality score
                    quality_score = 0
                    if splits_count >= 100:
                        quality_score += 25
                    if games_count >= 50:
                        quality_score += 25
                    if outcome_coverage >= 80:
                        quality_score += 25
                    if recent_splits >= 10:
                        quality_score += 25
                    
                    quality_emoji = "🟢" if quality_score >= 75 else "🟡" if quality_score >= 50 else "🔴"
                    click.echo(f"\n{quality_emoji} DATA QUALITY SCORE: {quality_score}/100")
                
        except Exception as e:
            click.echo(f"❌ Performance metrics failed: {e}")
        finally:
            try:
                if 'db_manager' in locals():
                    db_manager.close()
            except Exception:
                pass
    
    try:
        asyncio.run(show_performance())
    except Exception:
        click.echo("❌ Performance metrics failed")
        raise


@status_group.command('fix')
@click.option('--auto-approve', is_flag=True, help='Automatically approve fixes')
def fix_issues(auto_approve: bool):
    """🔧 Attempt to automatically fix common system issues"""
    
    async def run_fixes():
        click.echo("🔧 AUTOMATIC ISSUE RESOLUTION")
        click.echo("=" * 60)
        
        if not auto_approve:
            click.echo("⚠️  This will attempt to fix system issues automatically")
            if not click.confirm("Continue?"):
                click.echo("❌ Fix operation cancelled")
                return
        
        fixes_applied = []
        fix_errors = []
        
        try:
            # Check what needs fixing
            orchestrator = PipelineOrchestrator()
            system_state = await orchestrator.analyze_system_state()
            
            click.echo("🔍 Analyzing system for fixable issues...")
            
            # Fix 1: Stale data
            if system_state['needs_data_collection']:
                click.echo("\n🔧 Fixing stale data...")
                try:
                    from ...entrypoint import DataPipeline
                    
                    pipeline = DataPipeline(sport='mlb', sportsbook='circa', dry_run=False)
                    metrics = await pipeline.run()
                    
                    click.echo(f"   ✅ Fresh data collected ({metrics.get('parsed_records', 0)} records)")
                    fixes_applied.append("Fresh data collection")
                    
                except Exception as e:
                    click.echo(f"   ❌ Data collection failed: {e}")
                    fix_errors.append(f"Data collection: {e}")
            
            # Fix 2: Outdated backtesting
            if system_state['needs_backtesting']:
                click.echo("\n🔧 Updating backtesting results...")
                try:
                    enhanced_service = EnhancedBacktestingService(auto_collect_data=False)
                    results = await enhanced_service.run_daily_backtesting_pipeline()
                    
                    click.echo(f"   ✅ Backtesting updated ({results.total_strategies_analyzed} strategies)")
                    fixes_applied.append("Backtesting update")
                    
                except Exception as e:
                    click.echo(f"   ❌ Backtesting update failed: {e}")
                    fix_errors.append(f"Backtesting: {e}")
            
            # Fix 3: Database maintenance
            click.echo("\n🔧 Running database maintenance...")
            try:
                db_manager = get_db_manager()
                with db_manager.get_cursor() as cursor:
                    # Run VACUUM ANALYZE on main tables
                    for table in ['splits.raw_mlb_betting_splits', 'public.game_outcomes']:
                        try:
                            cursor.execute(f"VACUUM ANALYZE {table}")
                            click.echo(f"   ✅ Optimized {table}")
                        except Exception as e:
                            click.echo(f"   ⚠️  Could not optimize {table}: {e}")
                
                fixes_applied.append("Database optimization")
                
            except Exception as e:
                click.echo(f"   ❌ Database maintenance failed: {e}")
                fix_errors.append(f"Database maintenance: {e}")
            
            # Summary
            click.echo(f"\n📊 FIX SUMMARY:")
            click.echo(f"   ✅ Fixes Applied: {len(fixes_applied)}")
            click.echo(f"   ❌ Fix Errors: {len(fix_errors)}")
            
            if fixes_applied:
                click.echo(f"\n✅ SUCCESSFUL FIXES:")
                for fix in fixes_applied:
                    click.echo(f"   • {fix}")
            
            if fix_errors:
                click.echo(f"\n❌ FAILED FIXES:")
                for error in fix_errors:
                    click.echo(f"   • {error}")
            
            if fixes_applied and not fix_errors:
                click.echo(f"\n🎉 All issues fixed successfully!")
            elif fixes_applied:
                click.echo(f"\n⚠️  Some issues fixed, but errors remain")
            else:
                click.echo(f"\n❌ No fixes were successful")
                
        except Exception as e:
            click.echo(f"❌ Fix operation failed: {e}")
        finally:
            try:
                if 'orchestrator' in locals():
                    orchestrator.close()
            except Exception:
                pass
    
    try:
        asyncio.run(run_fixes())
    except Exception:
        click.echo("❌ Fix operation failed")
        raise 