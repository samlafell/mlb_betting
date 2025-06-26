"""
Enhanced detection commands with integrated pipeline.
"""

import click
import asyncio
from datetime import datetime
from pathlib import Path
from typing import Optional
import structlog

from ...services.pipeline_orchestrator import PipelineOrchestrator
from ...db.connection import get_db_manager

logger = structlog.get_logger(__name__)


@click.group()
def detection_group():
    """🎯 Enhanced betting opportunity detection commands."""
    pass


@detection_group.command('opportunities')
@click.option('--minutes', '-m', type=int, default=60,
              help='Minutes ahead to look for opportunities (default: 60)')
@click.option('--fresh-data/--use-existing', default=True,
              help='Collect fresh data before detection (default: enabled)')
@click.option('--run-backtesting/--skip-backtesting', default=True,
              help='Run backtesting before detection (default: enabled)')
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
def detect_opportunities(minutes: int, fresh_data: bool, run_backtesting: bool, 
                        format: str, output: Optional[Path], include_cross_market: bool, 
                        min_flip_confidence: float):
    """🎯 MASTER BETTING DETECTOR with full data pipeline"""
    
    async def run_detection():
        click.echo("🎯 MASTER BETTING DETECTOR")
        click.echo("=" * 60)
        click.echo("🤖 Using AI-optimized strategies with intelligent pipeline")
        
        if fresh_data:
            click.echo("📡 Fresh data collection: ENABLED")
        else:
            click.echo("📊 Using existing data: ENABLED")
            
        if run_backtesting:
            click.echo("🔬 Backtesting: ENABLED")
        else:
            click.echo("📋 Using existing backtesting: ENABLED")
            
        if include_cross_market:
            click.echo("🔀 Cross-market flips: ENABLED")
        
        try:
            # Initialize orchestrator
            orchestrator = PipelineOrchestrator()
            
            # Run smart pipeline
            results = await orchestrator.execute_smart_pipeline(
                detection_minutes=minutes,
                force_fresh_data=fresh_data,
                force_backtesting=run_backtesting
            )
            
            if format == "console":
                # Display execution summary
                click.echo(f"\n🚀 PIPELINE EXECUTION SUMMARY")
                click.echo(f"   ⏱️  Total Time: {results['total_execution_time']:.2f}s")
                click.echo(f"   🔧 Steps: {', '.join(results['steps_executed'])}")
                
                if results['errors']:
                    click.echo(f"   ❌ Errors: {len(results['errors'])}")
                    for error in results['errors']:
                        click.echo(f"      • {error}")
                
                if results['warnings']:
                    click.echo(f"   ⚠️  Warnings: {len(results['warnings'])}")
                    for warning in results['warnings']:
                        click.echo(f"      • {warning}")
                
                # Display detection results
                if results['detection_results']:
                    detection_results = results['detection_results']
                    total_opportunities = sum(
                        len(game_analysis.sharp_signals) + 
                        len(game_analysis.opposing_markets) + 
                        len(game_analysis.steam_moves) + 
                        len(game_analysis.book_conflicts)
                        for game_analysis in detection_results.games.values()
                    )
                    
                    click.echo(f"\n🎯 OPPORTUNITY DETECTION RESULTS")
                    click.echo(f"   🎮 Games Analyzed: {len(detection_results.games)}")
                    click.echo(f"   🚨 Total Opportunities: {total_opportunities}")
                    
                    # Display each game's opportunities
                    for game_key, game_analysis in detection_results.games.items():
                        away, home, game_time = game_key
                        opportunities = (
                            len(game_analysis.sharp_signals) + 
                            len(game_analysis.opposing_markets) + 
                            len(game_analysis.steam_moves) + 
                            len(game_analysis.book_conflicts)
                        )
                        
                        if opportunities > 0:
                            click.echo(f"\n   🎲 {away} @ {home}")
                            click.echo(f"      📅 {game_time.strftime('%Y-%m-%d %H:%M EST')}")
                            click.echo(f"      🎯 Opportunities: {opportunities}")
                            
                            if game_analysis.sharp_signals:
                                click.echo(f"         🔪 Sharp Signals: {len(game_analysis.sharp_signals)}")
                            if game_analysis.opposing_markets:
                                click.echo(f"         ⚔️  Opposing Markets: {len(game_analysis.opposing_markets)}")
                            if game_analysis.steam_moves:
                                click.echo(f"         🌊 Steam Moves: {len(game_analysis.steam_moves)}")
                            if game_analysis.book_conflicts:
                                click.echo(f"         📚 Book Conflicts: {len(game_analysis.book_conflicts)}")
                else:
                    click.echo(f"\n⚠️  No opportunity detection results available")
                
                # Display cross-market flips
                if include_cross_market and results['cross_market_flips']:
                    flips = results['cross_market_flips']['flips']
                    
                    if flips:
                        click.echo(f"\n🔀 CROSS-MARKET FLIP ANALYSIS")
                        click.echo("=" * 60)
                        click.echo(f"Found {len(flips)} cross-market flips with ≥{min_flip_confidence}% confidence")
                        
                        for i, flip in enumerate(flips, 1):
                            click.echo(f"\n🎯 FLIP #{i}: {flip.away_team} @ {flip.home_team}")
                            click.echo(f"   📅 Game: {flip.game_datetime.strftime('%Y-%m-%d %H:%M EST')}")
                            click.echo(f"   🔄 Type: {flip.flip_type.value.replace('_', ' ').title()}")
                            click.echo(f"   📊 Confidence: {flip.confidence_score:.1f}%")
                            click.echo(f"   💡 Strategy: {flip.strategy_recommendation}")
                            click.echo(f"   🧠 Reasoning: {flip.reasoning}")
                            
                            # Highlight high-confidence flips
                            if flip.confidence_score >= 80:
                                click.echo(f"   🔥 HIGH CONFIDENCE - STRONG BETTING OPPORTUNITY")
                            elif flip.confidence_score >= 70:
                                click.echo(f"   ✨ GOOD CONFIDENCE - SOLID BETTING OPPORTUNITY")
                            
                            # ⚠️ CRITICAL WARNING: This strategy is untested
                            click.echo(f"   ⚠️  WARNING: Cross-market flip strategies have NO backtesting results")
                            click.echo(f"   📊 Confidence is theoretical only - strategy performance unknown")
                            click.echo(f"   💡 Use small bet sizes until strategy is proven")
                    else:
                        click.echo(f"\n🔀 No cross-market flips found with ≥{min_flip_confidence}% confidence")
                
            elif format == "json":
                import json
                
                # Convert to JSON-serializable format
                json_output = {
                    'timestamp': datetime.now().isoformat(),
                    'pipeline_execution': {
                        'steps_executed': results['steps_executed'],
                        'execution_time_seconds': results['total_execution_time'],
                        'errors': results['errors'],
                        'warnings': results['warnings']
                    },
                    'detection_window_minutes': minutes,
                    'fresh_data_enabled': fresh_data,
                    'backtesting_enabled': run_backtesting,
                    'cross_market_enabled': include_cross_market,
                    'min_flip_confidence': min_flip_confidence
                }
                
                # Add detection results if available
                if results['detection_results']:
                    detection_results = results['detection_results']
                    json_games = {}
                    for game_key, game_analysis in detection_results.games.items():
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
                            'total_opportunities': (
                                len(game_analysis.sharp_signals) + 
                                len(game_analysis.opposing_markets) + 
                                len(game_analysis.steam_moves) + 
                                len(game_analysis.book_conflicts)
                            )
                        }
                    
                    json_output['opportunities'] = {
                        'total_games': len(detection_results.games),
                        'games': json_games,
                        'analysis_metadata': detection_results.analysis_metadata
                    }
                
                # Add cross-market flips if available
                if include_cross_market and results['cross_market_flips']:
                    flips = results['cross_market_flips']['flips']
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
                            'reasoning': flip.reasoning
                        })
                    
                    json_output['cross_market_flips'] = {
                        'count': len(flips),
                        'flips': json_flips
                    }
                
                json_str = json.dumps(json_output, indent=2)
                if output:
                    output.write_text(json_str)
                    click.echo(f"✅ Detection results saved to: {output}")
                else:
                    click.echo(json_str)
                    
        except Exception as e:
            click.echo(f"❌ Detection pipeline failed: {e}")
            raise
        finally:
            # Cleanup
            try:
                if 'orchestrator' in locals():
                    orchestrator.close()
            except Exception as cleanup_error:
                click.echo(f"⚠️  Cleanup warning: {cleanup_error}")
    
    try:
        asyncio.run(run_detection())
    except KeyboardInterrupt:
        click.echo("\n⚠️  Detection interrupted by user")
    except Exception:
        click.echo("❌ Detection failed")
        raise


@detection_group.command('smart-pipeline')
@click.option('--minutes', '-m', type=int, default=60,
              help='Minutes ahead to look for opportunities (default: 60)')
@click.option('--force-fresh', is_flag=True,
              help='Force fresh data collection regardless of age')
@click.option('--max-data-age', type=int, default=6,
              help='Maximum data age in hours before forcing collection (default: 6)')
def smart_pipeline(minutes: int, force_fresh: bool, max_data_age: int):
    """🧠 SMART PIPELINE - Automatically decides what needs to be run"""
    
    async def run_smart_pipeline():
        click.echo("🧠 SMART PIPELINE ORCHESTRATOR")
        click.echo("=" * 60)
        click.echo("🤖 Analyzing system state to determine optimal execution plan")
        
        try:
            orchestrator = PipelineOrchestrator()
            
            # Get recommendations first
            recommendations = await orchestrator.get_pipeline_recommendations()
            
            click.echo(f"\n📊 SYSTEM ANALYSIS:")
            click.echo(f"   🏥 System Health: {recommendations['system_health'].upper()}")
            click.echo(f"   🚨 Priority Level: {recommendations['priority_level'].upper()}")
            click.echo(f"   ⏱️  Estimated Runtime: {recommendations['estimated_runtime_minutes']} minutes")
            
            # Show immediate actions
            if recommendations['immediate_actions']:
                click.echo(f"\n🚀 IMMEDIATE ACTIONS PLANNED:")
                for action in recommendations['immediate_actions']:
                    click.echo(f"   ✅ {action['action'].title()}: {action['reason']}")
                    click.echo(f"      ⏱️  Est. {action['estimated_minutes']} minutes")
            
            # Show optional actions
            if recommendations['optional_actions']:
                click.echo(f"\n💡 OPTIONAL ACTIONS AVAILABLE:")
                for action in recommendations['optional_actions']:
                    click.echo(f"   💡 {action['action'].title()}: {action['reason']}")
            
            # Execute smart pipeline
            click.echo(f"\n🚀 EXECUTING SMART PIPELINE...")
            
            results = await orchestrator.execute_smart_pipeline(
                detection_minutes=minutes,
                force_fresh_data=force_fresh
            )
            
            # Display results
            click.echo(f"\n✅ SMART PIPELINE COMPLETED")
            click.echo(f"   ⏱️  Execution Time: {results['total_execution_time']:.2f}s")
            click.echo(f"   🔧 Steps Executed: {', '.join(results['steps_executed'])}")
            
            if results['errors']:
                click.echo(f"   ❌ Errors: {len(results['errors'])}")
                for error in results['errors']:
                    click.echo(f"      • {error}")
            
            if results['warnings']:
                click.echo(f"   ⚠️  Warnings: {len(results['warnings'])}")
                for warning in results['warnings']:
                    click.echo(f"      • {warning}")
            
            # Show step-by-step results
            if 'data_collection' in results['steps_executed']:
                metrics = results['data_collection_metrics']
                if metrics:
                    click.echo(f"\n📡 DATA COLLECTION RESULTS:")
                    click.echo(f"   📥 Records Processed: {metrics.get('parsed_records', 0)}")
                    click.echo(f"   🎯 Sharp Indicators: {metrics.get('sharp_indicators', 0)}")
            
            if 'backtesting' in results['steps_executed']:
                backtest = results['backtesting_results']
                if backtest:
                    click.echo(f"\n🔬 BACKTESTING RESULTS:")
                    click.echo(f"   📊 Strategies Analyzed: {backtest.total_strategies_analyzed}")
                    click.echo(f"   💰 Profitable Strategies: {backtest.profitable_strategies}")
            
            if 'detection' in results['steps_executed']:
                detection = results['detection_results']
                if detection:
                    total_opportunities = sum(
                        len(game_analysis.sharp_signals) + 
                        len(game_analysis.opposing_markets) + 
                        len(game_analysis.steam_moves) + 
                        len(game_analysis.book_conflicts)
                        for game_analysis in detection.games.values()
                    )
                    
                    click.echo(f"\n🎯 OPPORTUNITY DETECTION RESULTS:")
                    click.echo(f"   🎮 Games Analyzed: {len(detection.games)}")
                    click.echo(f"   🚨 Total Opportunities: {total_opportunities}")
                    
                    if total_opportunities > 0:
                        click.echo(f"\n💡 Run 'mlb-cli detect opportunities' for detailed analysis")
            
            # Cross-market flips summary
            if results['cross_market_flips']:
                flips = results['cross_market_flips']['flips']
                if flips:
                    click.echo(f"\n🔀 CROSS-MARKET FLIPS:")
                    click.echo(f"   🎯 High-Confidence Flips: {len(flips)}")
                    click.echo(f"   💡 Run 'mlb-cli cross-market-flips' for details")
            
            click.echo(f"\n🎉 Smart pipeline execution completed successfully!")
            
        except Exception as e:
            click.echo(f"❌ Smart pipeline failed: {e}")
            raise
        finally:
            # Cleanup
            try:
                if 'orchestrator' in locals():
                    orchestrator.close()
            except Exception as cleanup_error:
                click.echo(f"⚠️  Cleanup warning: {cleanup_error}")
    
    try:
        asyncio.run(run_smart_pipeline())
    except KeyboardInterrupt:
        click.echo("\n⚠️  Smart pipeline interrupted by user")
    except Exception:
        click.echo("❌ Smart pipeline failed")
        raise


@detection_group.command('recommendations')
@click.option('--minutes', '-m', type=int, default=60,
              help='Minutes ahead to look for opportunities (default: 60)')
def get_recommendations(minutes: int):
    """💡 Get intelligent recommendations for what should be run"""
    
    async def show_recommendations():
        click.echo("💡 SYSTEM RECOMMENDATIONS")
        click.echo("=" * 50)
        
        try:
            orchestrator = PipelineOrchestrator()
            recommendations = await orchestrator.get_pipeline_recommendations()
            
            # System health
            health = recommendations['system_health']
            health_emoji = {
                'excellent': '🟢',
                'good': '🟡', 
                'fair': '🟠',
                'poor': '🔴',
                'unknown': '⚪'
            }.get(health, '⚪')
            
            click.echo(f"{health_emoji} System Health: {health.upper()}")
            click.echo(f"🚨 Priority Level: {recommendations['priority_level'].upper()}")
            click.echo(f"⏱️  Estimated Runtime: {recommendations['estimated_runtime_minutes']} minutes")
            
            # Immediate actions
            if recommendations['immediate_actions']:
                click.echo(f"\n🚀 RECOMMENDED ACTIONS:")
                for i, action in enumerate(recommendations['immediate_actions'], 1):
                    click.echo(f"{i}. {action['action'].replace('_', ' ').title()}")
                    click.echo(f"   📝 Reason: {action['reason']}")
                    click.echo(f"   ⏱️  Time: ~{action['estimated_minutes']} minutes")
                    click.echo()
                
                click.echo(f"💡 Run 'mlb-cli detect smart-pipeline' to execute automatically")
            else:
                click.echo(f"\n✅ No immediate actions needed")
                click.echo(f"💡 System is ready for detection")
            
            # Optional actions
            if recommendations['optional_actions']:
                click.echo(f"\n💡 OPTIONAL IMPROVEMENTS:")
                for action in recommendations['optional_actions']:
                    click.echo(f"• {action['action'].replace('_', ' ').title()}: {action['reason']}")
                    if 'issues' in action:
                        for issue in action['issues']:
                            click.echo(f"    - {issue}")
            
            # Reasoning
            if recommendations['reasoning']:
                click.echo(f"\n🧠 SYSTEM REASONING:")
                for reason in recommendations['reasoning']:
                    click.echo(f"• {reason}")
                    
        except Exception as e:
            click.echo(f"❌ Failed to get recommendations: {e}")
        finally:
            try:
                if 'orchestrator' in locals():
                    orchestrator.close()
            except Exception:
                pass
    
    try:
        asyncio.run(show_recommendations())
    except Exception:
        click.echo("❌ Recommendations failed")
        raise 